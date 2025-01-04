import asyncio

import pendulum as pend
import ujson
from aioredis import Redis
from kafka import KafkaProducer


class ReminderDispatcher:
    """Class to fetch reminders from Redis and send them to Kafka."""

    def __init__(self, redis_url: str, kafka_config: dict):
        self.redis_client: Redis = None
        self.redis_url = redis_url
        self.kafka_producer = KafkaProducer(**kafka_config)

    async def connect_redis(self):
        """Connect to Redis."""
        self.redis_client = await Redis.from_url(self.redis_url)

    async def close_redis(self):
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()

    async def get_due_reminders(self):
        """Retrieve all due reminders from Redis."""
        keys = await self.redis_client.keys('war_reminder_*')
        due_reminders = []
        for key in keys:
            data = await self.redis_client.get(key)
            if data:
                reminder = ujson.loads(data)
                if reminder['run_date'] <= pend.now(tz=pend.UTC).int_timestamp:
                    due_reminders.append(reminder)
        return due_reminders

    async def send_to_kafka(self, reminder):
        """Send reminder to Kafka."""
        topic = 'reminders'
        job_id = reminder['job_id']
        try:
            self.kafka_producer.send(
                topic=topic,
                key=job_id.encode('utf-8'),
                value=ujson.dumps(reminder).encode('utf-8'),
                timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000),
            )
            print(f'Sent reminder to Kafka: {job_id}')
        except Exception as e:
            print(f'Failed to send reminder {job_id} to Kafka: {e}')

    async def process_reminders(self):
        """Process due reminders by sending them to Kafka and removing them."""
        due_reminders = await self.get_due_reminders()
        for reminder in due_reminders:
            await self.send_to_kafka(reminder)
            await self.redis_client.delete(reminder['job_id'])
            print(f"Processed and deleted reminder: {reminder['job_id']}")

    async def run(self):
        """Main loop to process reminders continuously."""
        await self.connect_redis()
        try:
            while True:
                await self.process_reminders()
                await asyncio.sleep(10)  # Check every 10 seconds
        finally:
            await self.close_redis()


async def main():
    redis_url = 'redis://localhost'
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],
        'value_serializer': lambda v: ujson.dumps(v).encode('utf-8'),
        'key_serializer': lambda k: k.encode('utf-8'),
    }
    dispatcher = ReminderDispatcher(redis_url, kafka_config)

    await dispatcher.run()


if __name__ == '__main__':
    asyncio.run(main())
