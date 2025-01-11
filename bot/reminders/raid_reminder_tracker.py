import asyncio
import json

import pendulum as pend
from asyncio_throttle import Throttler
from loguru import logger

from utility.classes_utils.raids_utils import is_raids, get_raid_log_entry
from utility.config import Config
from utility.utils import serialize


class RaidReminderTracker:
    def __init__(self, config, db_client, kafka_producer, is_raids_func=None):
        """Initialize the Raid Reminder Tracker."""
        self.logger = logger
        self.config = config
        self.db_client = db_client
        self.kafka_producer = kafka_producer
        self.coc_client = self.config.coc_client
        self.is_raids_func = is_raids_func or is_raids
        self.throttler = Throttler(rate_limit=100)
        self.batch_size = 100


    async def fetch_missing_members(self, clan_tag, clan_members, attack_threshold=0, townhall_levels=[], roles=[]):
        """Fetch members who haven't attacked yet from the Clash of Clans API."""
        try:
            async with self.throttler:  # Applique le throttling ici
                clan = await self.config.coc_client.get_clan(clan_tag)
            if not clan:
                self.logger.error(f"Clan {clan_tag} not found.")
                return []

            # Calculate the current weekend's starting timestamp
            weekend = pend.now(tz=pend.UTC).start_of('week').add(days=4).at(7, 0, 0).format('YYYY-MM-DD')

            # Retrieve the raid log entry for the weekend
            raid_log = await get_raid_log_entry(clan, weekend, limit=1, coc_client=self.config.coc_client,
                                                db_client=self.db_client)
            if not raid_log:
                return []

            # Create a lookup table for clan members by their tag
            clan_members_lookup = {member.tag: member for member in clan_members}

            # Collect missing members
            missing_clan_members = {}

            # Check members in the raid log
            for member in raid_log.members:
                attack_total = member.attack_limit + member.bonus_attack_limit
                if int(member.attack_count) < (int(attack_total) - int(attack_threshold)):
                    enriched_member = clan_members_lookup.get(member.tag)
                    if enriched_member and (
                            (not roles or str(enriched_member.role) in roles) and
                            (not townhall_levels or str(enriched_member.town_hall) in townhall_levels)
                    ):
                        missing_clan_members[member.tag] = {
                            "name": enriched_member.name,
                            "town_hall": enriched_member.town_hall,
                            "role": enriched_member.role,
                            "attacks": member.attack_count,
                            "total_attacks": attack_total
                        }

            # Add clan members missing from the raid log
            raid_log_member_tags = {member.tag for member in raid_log.members}
            for member in clan_members:
                if member.tag not in raid_log_member_tags and (
                        (not roles or str(member.role) in roles) and
                        (not townhall_levels or str(member.town_hall) in townhall_levels)
                ):
                    missing_clan_members[member.tag] = {
                        "name": member.name,
                        "town_hall": member.town_hall,
                        "role": member.role,
                        "attacks": "0",
                        "total_attacks": "5"
                    }

            # self.logger.info(f"Found {len(missing_clan_members)} missing members for clan {clan_tag}.")
            return missing_clan_members

        except Exception as e:
            logger.error(f"Error fetching missing members for clan {clan_tag}: {e}")
            return []

    def chunked(self, iterable):
        """Divide an iterable into chunks of a given size."""
        for i in range(0, len(iterable), self.batch_size):
            yield iterable[i:i + self.batch_size]

    async def track_raid_reminders(self):
        """Track and process raid reminders in parallel."""
        try:
            now = pend.now(tz=pend.UTC)
            raid_end_time = now.start_of('week').add(days=7, hours=7)  # Monday 7:00 UTC
            remaining_hours = (raid_end_time - now).in_hours()

            if remaining_hours <= 0:
                logger.info('Raids have ended. No reminders to send.')
                return

            remaining_time_formatted = f'{int(remaining_hours)} hr'
            self.logger.info(f'Fetching reminders for {remaining_time_formatted}.')

            # Fetch matching reminders from the database
            reminders = await self.db_client.reminders.find(
                {'type': 'Clan Capital', 'time': remaining_time_formatted}
            ).to_list(length=None)

            if not reminders:
                logger.info(f'No reminders found for {remaining_time_formatted}.')

            for batch in self.chunked(reminders):
                tasks = [self.process_reminder(reminder) for reminder in batch]
                await asyncio.gather(*tasks)
            self.logger.info(f"Processed {len(reminders)} reminders in batches.")
            return
        except Exception as e:
            logger.error(f"Error tracking raid reminders: {e}")

    async def process_reminder(self, reminder):
        """Process a single raid reminder."""
        try:
            clan_tag = reminder.get('clan')

            threshold = reminder.get('attack_threshold', 0)
            roles = reminder.get('roles', [])
            townhall_levels = reminder.get('townhalls', [])

            clan = await self.config.coc_client.get_clan(clan_tag)

            # Fetch members who haven't attacked yet
            missing_members = await self.fetch_missing_members(clan_tag, clan.members, threshold, townhall_levels,
                                                               roles)
            if missing_members:
                await self.send_to_kafka(reminder, missing_members)
            # logger.info(f'Processed reminder for clan {clan_tag} with {len(missing_members)} missing members.')
        except Exception as e:
            logger.error(f"Error processing reminder {reminder.get('_id')}: {e}")

    async def send_to_kafka(self, reminder, members):
        """Send a reminder to Kafka."""
        try:
            topic = reminder.get('type', 'reminders')
            key = reminder.get('clan', 'unknown').encode('utf-8')  #
            sanitized_reminder = json.loads(json.dumps(reminder, default=serialize))
            sanitized_members = json.loads(json.dumps(members, default=serialize))
            value = json.dumps({
                'reminder': sanitized_reminder,
                'members': sanitized_members
            }).encode('utf-8')

            await self.kafka_producer.send(topic, key=key, value=value)
        except Exception as e:
            logger.error(f"Error sending reminder to Kafka: {e}")

    async def run(self):
        try:
            logger.info('Raid tracking: Raid weekend has started. Start tracking.')
            while self.is_raids_func():
                start_time = pend.now(tz=pend.UTC)
                await self.track_raid_reminders()
                now = pend.now(tz=pend.UTC)
                next_hour = now.add(hours=1).start_of('hour')
                sleep_time = (next_hour - now).total_seconds()
                total_time = (pend.now(tz=pend.UTC) - start_time).total_seconds()
                logger.info(
                    f'Processed raid reminders in {total_time:.2f} seconds. Next execution scheduled at {next_hour}.')
                await asyncio.sleep(sleep_time)
            logger.info(
                'Raids tracking: Raid weekend has ended. Stop tracking.'
            )

        except Exception as e:
            logger.error(f'Error in raid tracking loop: {e}')
