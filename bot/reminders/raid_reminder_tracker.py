import asyncio

import pendulum as pend
from loguru import logger

from utility.classes_utils.raids_utils import is_raids
from utility.config import Config


class RaidReminderTracker:
    def __init__(self, is_raids_func=None):
        """Initialize the Raid Reminder Tracker."""
        self.logger = logger
        self.db_client = None
        self.kafka_producer = None
        self.config = Config()
        self.is_raids_func = is_raids_func or is_raids

    async def initialize(self):
        """Initialize dependencies."""
        await self.config.initialize()
        self.db_client = self.config.get_mongo_database()
        self.kafka_producer = self.config.get_kafka_producer()
        logger.info('Dependencies initialized.')

    async def cleanup(self):
        """Clean up resources."""
        if self.db_client:
            await self.db_client.close()
        if self.kafka_producer:
            await self.kafka_producer.stop()
        logger.info('Cleaned up resources.')

    def get_time_until_next_raid(self):
        """Calculate the time until the next raid window starts."""
        now = pend.now(tz=pend.UTC)
        next_raid_start = now.start_of('week').add(
            days=4, hours=7
        )  # Friday 7:00 UTC
        if now >= next_raid_start:  # If we're past this week's Friday 7:00 UTC
            next_raid_start = next_raid_start.add(
                weeks=1
            )  # Move to next Friday
        sleep_time = (next_raid_start - now).total_seconds()
        logger.info(f'Time until next raid: {sleep_time // 3600} hours.')
        return sleep_time

    async def track_raid_reminders(self):
        """Track and process raid reminders."""
        now = pend.now(tz=pend.UTC)
        raid_end_time = now.start_of('week').add(
            days=7, hours=7
        )  # Monday 7:00 UTC
        remaining_hours = (raid_end_time - now).in_hours()

        if remaining_hours <= 0:
            logger.info('Raids have ended. No reminders to send.')
            return

        # Convert remaining time to the correct format (e.g., "XX hr")
        remaining_time_formatted = f'{int(remaining_hours)} hr'

        # Fetch matching reminders from the database
        reminders = await self.db_client.reminders.find(
            {'type': 'Clan Capital', 'time': remaining_time_formatted}
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
        """Run the raid tracking loop."""
        await self.initialize()

        try:
            while True:
                if self.is_raids_func():
                    logger.info('Raid tracking is active.')
                    while self.is_raids_func():
                        now = pend.now(tz=pend.UTC)
                        next_hour = now.add(hours=1).start_of('hour')
                        sleep_time = (next_hour - now).total_seconds()

                        logger.info(
                            f'Next execution scheduled at {next_hour}.'
                        )
                        await asyncio.sleep(sleep_time)

                        # Execute the tracking
                        await self.track_raid_reminders()

                    logger.info(
                        'Raids have ended. Calculating time until next raid...'
                    )
                else:
                    sleep_time = self.get_time_until_next_raid()
                    logger.info(
                        f'Sleeping until the next raid window starts...'
                    )
                    await asyncio.sleep(sleep_time)

        except Exception as e:
            logger.error(f'Error in raid tracking loop: {e}')
        finally:
            await self.cleanup()


if __name__ == '__main__':
    tracker = RaidReminderTracker()
    asyncio.run(tracker.run())
