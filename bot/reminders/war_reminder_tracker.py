import asyncio

import pendulum as pend
import ujson
from bson import ObjectId
from kafka.errors import KafkaError
from loguru import logger
from pymongo.errors import PyMongoError

from utility.config import Config


class WarReminderTracker:
    def __init__(self):
        """Initialize the ReminderDispatcher."""
        self.logger = logger
        self.db_client = None
        self.kafka_producer = None
        self.config = Config()

    async def initialize(self):
        """Initialize dependencies."""
        await self.config.initialize()
        self.db_client = self.config.get_mongo_database()
        self.kafka_producer = self.config.get_kafka_producer()
        logger.info('Dependencies initialized.')

    async def fetch_due_reminders(self, lookahead_minutes=10):
        """Fetch reminders that are due for execution, including those due in the next few minutes.

        Args:
            lookahead_minutes (int): Number of minutes ahead to include in the due reminders.

        Returns:
            List[dict]: A list of reminders due for execution.
        """
        now_timestamp = pend.now(tz=pend.UTC).int_timestamp
        lookbehind_timestamp = (
            pend.now(tz=pend.UTC).subtract(minutes=10).int_timestamp
        )

        try:
            # Fetch reminders that are due now or within the lookahead period
            reminders = await self.db_client.active_reminders.find(
                {
                    'run_date': {
                        '$gte': lookbehind_timestamp,
                        '$lt': now_timestamp,
                    }
                }
            ).to_list(length=None)

            return reminders
        except PyMongoError as e:
            print(f'Error fetching reminders from MongoDB: {e}')
            return []

    async def send_to_kafka(self, reminder):
        """Send a reminder to Kafka and remove it from MongoDB.

        Args:
            reminder: The reminder document to send.
        """
        try:
            # Convert the _id back to ObjectId if it's a string
            reminder_id_object = reminder['_id']

            # Serialize the reminder data and send to Kafka
            topic = reminder.get('type', 'reminders')
            key = reminder.get('job_id', 'unknown').encode('utf-8')
            value = ujson.dumps(reminder).encode('utf-8')
            run_date = pend.from_timestamp(reminder['run_date'], tz=pend.UTC)

            self.kafka_producer.send(
                topic,
                key=key,
                value=value,
                timestamp_ms=run_date.int_timestamp,
            )
            self.logger.info(
                f"Sent reminder {reminder_id_object} ({reminder['job_id']}) to Kafka. Run date: {run_date}"
            )

            try:
                # Delete the reminder from the `active_reminders` collection
                result = await self.db_client.active_reminders.delete_one(
                    {'reminder_id': ObjectId(reminder_id_object)}
                )
                if result.deleted_count > 0:
                    self.logger.info(
                        f"Removed reminder {reminder['job_id']} from MongoDB."
                    )
                else:
                    self.logger.warning(
                        f"Failed to remove reminder {reminder_id_object} ({reminder['job_id']}) from MongoDB: Not found."
                    )
            except Exception as e:
                self.logger.error(
                    f"Error while removing reminder {reminder['job_id']} from MongoDB: {e}"
                )

        except KafkaError as e:
            self.logger.error(
                f"Error sending reminder {reminder['job_id']} to Kafka: {e}"
            )
        except PyMongoError as e:
            self.logger.error(
                f"Error removing reminder {reminder['job_id']} from MongoDB: {e}"
            )

    async def dispatch_reminders(self):
        """Fetch and send reminders to Kafka."""
        reminders = await self.fetch_due_reminders()
        if reminders:
            for reminder in reminders:
                try:
                    # Fetch detailed reminder information from the `reminders` collection
                    reminder_details = await self.db_client.reminders.find_one(
                        {'_id': reminder.get('reminder_id')}
                    )

                    if not reminder_details:
                        self.logger.warning(
                            f"Reminder details not found for reminder_id: {reminder.get('reminder_id')}"
                        )
                        continue

                    # Merge additional details into the reminder before sending to Kafka
                    enriched_reminder = {**reminder, **reminder_details}

                    # Convert ObjectId fields to strings for JSON serialization, except for '_id'
                    for key, value in enriched_reminder.items():
                        if isinstance(value, ObjectId):
                            enriched_reminder[key] = str(value)

                    # Send the enriched reminder to Kafka
                    await self.send_to_kafka(enriched_reminder)

                except Exception as e:
                    self.logger.error(
                        f"Error processing reminder {reminder.get('job_id')}: {e}"
                    )
        else:
            self.logger.info('No reminders due for dispatch.')

    async def run(self):
        """Run the ReminderDispatcher."""
        try:
            # Initialize configuration and dependencies
            await self.initialize()

            # Main loop to dispatch reminders every 10 seconds
            while True:
                await self.dispatch_reminders()
                await asyncio.sleep(10)
        except Exception as e:
            self.logger.error(f'An error occurred in ReminderDispatcher: {e}')


if __name__ == '__main__':
    dispatcher = WarReminderTracker()
    asyncio.run(dispatcher.run())
