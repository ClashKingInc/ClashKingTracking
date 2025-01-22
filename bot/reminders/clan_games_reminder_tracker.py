import asyncio
import json

import pendulum as pend
from asyncio_throttle import Throttler
from loguru import logger

from utility.classes_utils.clan_games_utils import is_clan_games
from utility.utils import is_member_eligible, serialize


class ClanGamesReminderTracker:
    def __init__(self, config, db_client, kafka_producer, is_clan_games_func=None):
        """Initialize the Clan Games Reminder Tracker."""
        self.logger = logger
        self.config = config
        self.db_client = db_client
        self.kafka_producer = kafka_producer
        self.coc_client = self.config.coc_client
        self.is_clan_games_func = is_clan_games_func or is_clan_games
        self.throttler = Throttler(rate_limit=100)
        self.batch_size = 100

    async def fetch_missing_members(
        self, clan_tag, clan_members, point_threshold=0, townhall_levels=None, roles=None
    ):
        """Fetch members who haven't met the point threshold from the Clash of Clans API and the DB."""
        try:
            async with self.throttler:
                clan = await self.config.coc_client.get_clan(clan_tag)

            if not clan:
                return {}

            current_month = (
                pend.now(tz=pend.UTC).start_of("month").format("YYYY-MM")
            )

            eligible_members = [
                member.tag
                for member in clan_members
                if is_member_eligible(member, roles, townhall_levels)
            ]

            query = {
                "tag": {"$in": eligible_members},
                f"clan_games.{current_month}.clan": clan_tag,
                f"clan_games.{current_month}.points": {"$lte": point_threshold},
            }

            projection = {
                "tag": 1,
                f"clan_games.{current_month}.points": 1,
            }

            db_results = await self.db_client.player_stats.find(query, projection).to_list(length=None)

            missing_clan_members = {}
            for member in clan_members:
                if member.tag in eligible_members:
                    db_member = next((m for m in db_results if m["tag"] == member.tag), None)
                    points = (
                        db_member["clan_games"][current_month]["points"]
                        if db_member
                        else 0
                    )

                    if points <= point_threshold:
                        missing_clan_members[member.tag] = {
                            "name": member.name,
                            "town_hall": member.town_hall,
                            "role": member.role,
                            "points": points,
                        }

            return missing_clan_members
        except Exception as e:
            self.logger.error(f"Error fetching missing members for clan {clan_tag}: {e}")
            return {}

    def chunked(self, iterable):
        """Divide an iterable into chunks of a given size."""
        for i in range(0, len(iterable), self.batch_size):
            yield iterable[i : i + self.batch_size]

    async def track_clan_games_reminders(self):
        """Track and process clan games reminders."""
        try:
            now = pend.now(tz=pend.UTC)
            clan_games_end_time = now.start_of("month").add(days=28, hours=9)
            remaining_hours = (clan_games_end_time - now).in_hours()

            if remaining_hours <= 0:
                self.logger.info("Clan Games have ended. No reminders to send.")
                return

            remaining_time_formatted = f"{int(remaining_hours)} hr"

            reminders = await self.db_client.reminders.find(
                {"type": "Clan Games", "time": "24 hr"}
            ).to_list(length=None)

            if not reminders:
                self.logger.info(f"No reminders found for {remaining_time_formatted}.")
                return

            for batch in self.chunked(reminders):
                tasks = [self.process_reminder(reminder) for reminder in batch]
                await asyncio.gather(*tasks)

            self.logger.info(f"Processed {len(reminders)} reminders in batches.")
        except Exception as e:
            self.logger.error(f"Error tracking raid reminders: {e}")

    async def process_reminder(self, reminder):
        """Process a single clan games reminder."""
        try:
            clan_tag = reminder.get("clan")

            threshold = reminder.get("point_threshold", 0)
            roles = reminder.get("roles", [])
            townhall_levels = reminder.get("townhalls", [])

            clan = await self.config.coc_client.get_clan(clan_tag)
            missing_members = await self.fetch_missing_members(
                clan_tag, clan.members, threshold, townhall_levels, roles
            )

            if missing_members:
                await self.send_to_kafka(reminder, missing_members)
        except Exception as e:
            self.logger.error(f"Error processing reminder {reminder.get('_id')}: {e}")

    async def send_to_kafka(self, reminder, members):
        """Send a reminder to Kafka."""
        try:
            topic = reminder.get("type", "reminders")
            key = reminder.get("clan", "unknown").encode("utf-8")

            sanitized_reminder = json.loads(json.dumps(reminder, default=serialize))
            sanitized_members = json.loads(json.dumps(members, default=serialize))

            value = json.dumps(
                {"reminder": sanitized_reminder, "members": sanitized_members}
            ).encode("utf-8")

            await self.kafka_producer.send(topic, key=key, value=value)
        except Exception as e:
            self.logger.error(f"Error sending reminder to Kafka: {e}")

    async def run(self):
        """Run the clan games tracking loop."""
        try:
            self.logger.info("Clan Games tracking: Clan Games event has started. Start tracking.")

            while self.is_clan_games_func():
                start_time = pend.now(tz=pend.UTC)
                await self.track_clan_games_reminders()

                now = pend.now(tz=pend.UTC)
                next_hour = now.add(hours=1).start_of("hour")
                sleep_time = (next_hour - now).total_seconds()

                total_time = (pend.now(tz=pend.UTC) - start_time).total_seconds()
                self.logger.info(
                    f"Processed clan games reminders in {total_time:.2f} seconds. Next execution scheduled at {next_hour}."
                )

                await asyncio.sleep(sleep_time)

            self.logger.info("Clan Games tracking: Clan Games event has ended. Stop tracking.")
        except Exception as e:
            self.logger.error(f"Error in clan games tracking loop: {e}")
