import asyncio
import pendulum

from bot.reminders.clan_games_reminder_tracker import ClanGamesReminderTracker
from raid_reminder_tracker import RaidReminderTracker
from utility.classes_utils.clan_games_utils import get_time_until_next_clan_games
from utility.classes_utils.raids_utils import get_time_until_next_raid, format_seconds_as_date
from war_reminder_tracker import WarReminderTracker
from inactivity_reminder_tracker import InactivityReminderTracker
from utility.config import Config, TrackingType
from loguru import logger


class ReminderManager:
    def __init__(self, reminder_config):
        self.config = reminder_config
        self.db_client = None
        self.kafka_producer = None
        self.raid_tracker = None
        self.war_tracker = None
        self.clan_games_tracker = None
        self.inactivity_tracker = None
        self.logger = logger

    async def init(self):
        """Asynchronously initialize trackers and dependencies."""
        await self.config.get_coc_client()
        self.db_client = self.config.get_mongo_database()
        self.kafka_producer = self.config.get_kafka_producer()

        self.raid_tracker = RaidReminderTracker(
            config=self.config, db_client=self.db_client, kafka_producer=self.kafka_producer
        )
        self.war_tracker = WarReminderTracker(
            config=self.config, db_client=self.db_client, kafka_producer=self.kafka_producer
        )
        self.clan_games_tracker = ClanGamesReminderTracker(
            config=self.config, db_client=self.db_client, kafka_producer=self.kafka_producer
        )
        self.inactivity_tracker = InactivityReminderTracker(
            config=self.config, db_client=self.db_client, kafka_producer=self.kafka_producer
        )

    async def cleanup(self):
        """Clean up resources."""
        if self.config.coc_client:
            await self.config.coc_client.close()
        if self.db_client:
            await self.db_client.close()
        if self.kafka_producer:
            await self.kafka_producer.close()
        self.logger.info('Cleaned up shared resources.')

    async def run(self):
        """Manage the execution of all trackers concurrently."""
        try:
            tasks = [
                asyncio.create_task(self.run_raid_tracker()),
                asyncio.create_task(self.run_war_tracker()),
                asyncio.create_task(self.run_inactivity_tracker()),
                asyncio.create_task(self.run_clan_games_tracker())
            ]
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            self.logger.info("Tasks were cancelled.")
        except Exception as e:
            self.logger.error(f"Error in ReminderManager: {e}")
        finally:
            await self.cleanup()

    async def run_raid_tracker(self):
        if self.raid_tracker.is_raids_func():
            await self.raid_tracker.run()
        else:
            sleep_time = get_time_until_next_raid()
            date = format_seconds_as_date(sleep_time)
            self.logger.info(f'Raid tracker: Sleeping until raids start at {date}...')
            await asyncio.sleep(sleep_time)

    async def run_clan_games_tracker(self):
        if self.clan_games_tracker.is_clan_games_func():
            await self.clan_games_tracker.run()
        else:
            sleep_time = get_time_until_next_clan_games()
            date = format_seconds_as_date(sleep_time)
            self.logger.info(f'Clan games tracker: Sleeping until clan games start at {date}...')
            await asyncio.sleep(sleep_time)

    async def run_war_tracker(self):
        await self.war_tracker.run()

    async def run_inactivity_tracker(self):
        while True:
            current_time = pendulum.now('UTC')
            minutes_to_wait = 60 - current_time.minute
            seconds_to_wait = (minutes_to_wait * 60) - current_time.second
            date = format_seconds_as_date(seconds_to_wait)
            self.logger.info(f'Inactivity tracker: Sleeping until next hour at {date}...')
            await asyncio.sleep(seconds_to_wait)
            await self.inactivity_tracker.run()


if __name__ == "__main__":
    async def main():
        config = Config(TrackingType.BOT_CLAN)
        manager = ReminderManager(reminder_config=config)
        await manager.init()
        await manager.run()

    asyncio.run(main())
