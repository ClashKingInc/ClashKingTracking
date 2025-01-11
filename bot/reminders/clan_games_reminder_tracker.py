import asyncio

import pendulum as pend
from loguru import logger

from utility.config import Config
from utility.classes_utils.clan_games_utils import is_clan_games


class ClanGamesReminderTracker:
    def __init__(self, config, db_client, kafka_producer, is_clan_games_func=None):
        """Initialize the Clan Games Reminder Tracker."""
        self.logger = logger
        self.config = config
        self.db_client = db_client
        self.kafka_producer = kafka_producer
        self.coc_client = self.config.coc_client
        self.is_clan_games_func = is_clan_games_func or is_clan_games

    async def track_clan_games_reminders(self):
        """Track and process clan games reminders."""
        now = pend.now(tz=pend.UTC)
        clan_games_end_time = now.start_of('month').add(days=28, hours=9)  # 28th 9:00 UTC
        remaining_hours = (clan_games_end_time - now).in_hours()

        if remaining_hours <= 0:
            logger.info('Clan Games have ended. No reminders to send.')
            return

        # Convert remaining time to the correct format (e.g., "XX hr")
        remaining_time_formatted = f'{int(remaining_hours)} hr'

        # Fetch matching reminders from the database
        reminders = await self.db_client.reminders.find(
            {'type': 'Clan Games', 'time': remaining_time_formatted}
        ).to_list(length=None)

        if not reminders:
            logger.info(f'No reminders found for {remaining_time_formatted}.')
            return

        # Send reminders to Kafka
        for reminder in reminders:
            try:
                await self.send_to_kafka(reminder)
                logger.info(f'Sent reminder: {reminder}')
            except Exception as e:
                logger.error(f"Error sending reminder {reminder['_id']}: {e}")

    async def send_to_kafka(self, reminder):
        """Send a reminder to Kafka."""
        topic = reminder.get('type', 'reminders')
        key = reminder.get('clan', 'unknown').encode('utf-8')
        value = reminder  # Serialize this to JSON if required
        await self.kafka_producer.send(topic, key=key, value=value)
        logger.info(f'Sent reminder to Kafka: {reminder}')

    async def run(self):
        """Run the clan games tracking loop."""
        await self.initialize()

        try:
            logger.info('Clan Games tracking: Clan Games event has started. Start tracking.')
            while self.is_clan_games_func():
                start_time = pend.now(tz=pend.UTC)
                await self.track_clan_games_reminders()
                now = pend.now(tz=pend.UTC)
                next_hour = now.add(hours=1).start_of('hour')
                sleep_time = (next_hour - now).total_seconds()
                total_time = (pend.now(tz=pend.UTC) - start_time).total_seconds()
                logger.info(
                    f'Processed clan games reminders in {total_time:.2f} seconds. Next execution scheduled at {next_hour}.')
                await asyncio.sleep(sleep_time)
            logger.info(
                'Clan Games tracking: Clan Games event has ended. Stop tracking.'
            )

        except Exception as e:
            logger.error(f'Error in clan games tracking loop: {e}')

