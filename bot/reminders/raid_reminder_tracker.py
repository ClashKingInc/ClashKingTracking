import asyncio
import json

import pendulum as pend
from asyncio_throttle import Throttler
from loguru import logger

from utility.classes_utils.raids_utils import (
    calculate_current_weekend,
    get_raid_log_entry,
    is_raids,
)
from utility.utils import is_member_eligible, serialize


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

    async def fetch_missing_members(
        self,
        clan_tag,
        clan_members,
        attack_threshold=0,
        townhall_levels=None,
        roles=None,
    ):
        """Fetch members who haven't attacked yet from the Clash of Clans API."""
        try:
            # Initialize mutable defaults if None
            if townhall_levels is None:
                townhall_levels = []
            if roles is None:
                roles = []

            clan = await self._get_clan(clan_tag)
            if not clan:
                return []

            weekend = calculate_current_weekend()
            raid_log = await self._get_raid_log(clan, weekend)
            if not raid_log:
                return []

            missing_members = self._identify_missing_members(
                raid_log,
                clan_members,
                attack_threshold,
                townhall_levels,
                roles,
            )

            return missing_members
        except Exception as e:
            self.logger.error(
                f'Error fetching missing members for clan {clan_tag}: {e}'
            )
            return []

    async def _get_clan(self, clan_tag):
        """Retrieve clan details from the API."""
        try:
            async with self.throttler:
                return await self.config.coc_client.get_clan(clan_tag)
        except Exception as e:
            self.logger.error(f'Error fetching clan {clan_tag}: {e}')
            return None

    async def _get_raid_log(self, clan, weekend):
        """Retrieve the raid log entry for the weekend."""
        try:
            return await get_raid_log_entry(
                clan,
                weekend,
                limit=1,
                coc_client=self.config.coc_client,
                db_client=self.db_client,
            )
        except Exception as e:
            self.logger.error(
                f'Error fetching raid log for clan {clan.tag}: {e}'
            )
            return None

    def _identify_missing_members(
        self, raid_log, clan_members, attack_threshold, townhall_levels, roles
    ):
        """Identify members who have not attacked or are missing from the raid log."""
        clan_members_lookup = {member.tag: member for member in clan_members}
        missing_clan_members = {}

        for member in raid_log.members:
            attack_total = member.attack_limit + member.bonus_attack_limit
            if int(member.attack_count) < (
                int(attack_total) - int(attack_threshold)
            ):
                enriched_member = clan_members_lookup.get(member.tag)
                if enriched_member and is_member_eligible(
                    enriched_member, roles, townhall_levels
                ):
                    missing_clan_members[
                        member.tag
                    ] = self._build_missing_member_entry(
                        enriched_member, member.attack_count, attack_total
                    )

        raid_log_member_tags = {member.tag for member in raid_log.members}
        for member in clan_members:
            if member.tag not in raid_log_member_tags and is_member_eligible(
                member, roles, townhall_levels
            ):
                missing_clan_members[
                    member.tag
                ] = self._build_missing_member_entry(member, '0', '5')

        return missing_clan_members

    @staticmethod
    def _build_missing_member_entry(member, attack_count, total_attacks):
        """Build the entry for a missing member."""
        return {
            'name': member.name,
            'town_hall': member.town_hall,
            'role': member.role,
            'attacks': attack_count,
            'total_attacks': total_attacks,
        }

    def chunked(self, iterable):
        """Divide an iterable into chunks of a given size."""
        for i in range(0, len(iterable), self.batch_size):
            yield iterable[i : i + self.batch_size]

    async def track_raid_reminders(self):
        """Track and process raid reminders in parallel."""
        try:
            now = pend.now(tz=pend.UTC)
            raid_end_time = now.start_of('week').add(
                days=7, hours=7
            )  # Monday 7:00 UTC
            remaining_hours = (raid_end_time - now).in_hours()

            if remaining_hours <= 0:
                logger.info('Raids have ended. No reminders to send.')
                return

            remaining_time_formatted = f'{int(remaining_hours)} hr'
            self.logger.info(
                f'Fetching reminders for {remaining_time_formatted}.'
            )

            # Fetch matching reminders from the database
            reminders = await self.db_client.reminders.find(
                {'type': 'Clan Capital', 'time': remaining_time_formatted}
            ).to_list(length=None)

            if not reminders:
                logger.info(
                    f'No reminders found for {remaining_time_formatted}.'
                )

            for batch in self.chunked(reminders):
                tasks = [self.process_reminder(reminder) for reminder in batch]
                await asyncio.gather(*tasks)
            self.logger.info(
                f'Processed {len(reminders)} reminders in batches.'
            )
            return
        except Exception as e:
            logger.error(f'Error tracking raid reminders: {e}')

    async def process_reminder(self, reminder):
        """Process a single raid reminder."""
        try:
            clan_tag = reminder.get('clan')

            threshold = reminder.get('attack_threshold', 0)
            roles = reminder.get('roles', [])
            townhall_levels = reminder.get('townhalls', [])

            clan = await self.config.coc_client.get_clan(clan_tag)

            # Fetch members who haven't attacked yet
            missing_members = await self.fetch_missing_members(
                clan_tag, clan.members, threshold, townhall_levels, roles
            )
            if missing_members:
                await self.send_to_kafka(reminder, missing_members)
            # logger.info(f'Processed reminder for clan {clan_tag} with {len(missing_members)} missing members.')
        except Exception as e:
            logger.error(
                f"Error processing reminder {reminder.get('_id')}: {e}"
            )

    async def send_to_kafka(self, reminder, members):
        """Send a reminder to Kafka."""
        try:
            topic = reminder.get('type', 'reminders')
            key = reminder.get('clan', 'unknown').encode('utf-8')  #
            sanitized_reminder = json.loads(
                json.dumps(reminder, default=serialize)
            )
            sanitized_members = json.loads(
                json.dumps(members, default=serialize)
            )
            value = json.dumps(
                {'reminder': sanitized_reminder, 'members': sanitized_members}
            ).encode('utf-8')

            await self.kafka_producer.send(topic, key=key, value=value)
        except Exception as e:
            logger.error(f'Error sending reminder to Kafka: {e}')

    async def run(self):
        try:
            logger.info(
                'Raid tracking: Raid weekend has started. Start tracking.'
            )
            while self.is_raids_func():
                start_time = pend.now(tz=pend.UTC)
                await self.track_raid_reminders()
                now = pend.now(tz=pend.UTC)
                next_hour = now.add(hours=1).start_of('hour')
                sleep_time = (next_hour - now).total_seconds()
                total_time = (
                    pend.now(tz=pend.UTC) - start_time
                ).total_seconds()
                logger.info(
                    f'Processed raid reminders in {total_time:.2f} seconds. Next execution scheduled at {next_hour}.'
                )
                await asyncio.sleep(sleep_time)
            logger.info(
                'Raids tracking: Raid weekend has ended. Stop tracking.'
            )

        except Exception as e:
            logger.error(f'Error in raid tracking loop: {e}')
