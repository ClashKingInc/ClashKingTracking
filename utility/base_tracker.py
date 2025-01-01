import asyncio
import sentry_sdk
from kafka import KafkaProducer
import ujson
from utility.classes import MongoDatabase
import pendulum as pend
from sentry_sdk.integrations.asyncio import AsyncioIntegration

from bot.dev.kafka_mock import MockKafkaProducer
from utility.utils import sentry_filter
from utility.config import Config

class BaseTracker:
    """Base class for tracking functionality."""

    def __init__(self, config, producer=None, max_concurrent_requests=1000, batch_size=500):
        self.config = config
        self.producer = producer or KafkaProducer(bootstrap_servers=["85.10.200.219:9092"])
        self.db_client = MongoDatabase(
            stats_db_connection=config.stats_mongodb,
            static_db_connection=config.static_mongodb,
        )
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.message_count = 0
        self.batch_size = batch_size
        self.coc_client = None

    async def initialize(self):
        """Initialize the CoC client."""
        self.coc_client = self.config.coc_client
        if not self.coc_client:
            raise RuntimeError("CoC client is not initialized in config.")

    async def track(self, items):
        """Track items in batches."""
        self.message_count = 0  # Reset message count
        for i in range(0, len(items), self.batch_size):
            batch = items[i:i + self.batch_size]
            print(f"Processing batch {i // self.batch_size + 1} of {len(items) // self.batch_size + 1}.")
            await self._track_batch(batch)

        sentry_sdk.add_breadcrumb(message="Finished tracking all clans.", level="info")
        print("Finished tracking all clans.")

    async def _track_batch(self, batch):
        """Track a batch of items."""
        async with self.semaphore:
            tasks = [self._track_item(item) for item in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    self._handle_exception("Error in tracking task", result)
        print(f"Finished tracking batch of {len(batch)} clans.")  # Added print

    async def _track_item(self, item):
        """Override this method in child classes."""
        raise NotImplementedError("This method should be overridden in child classes.")

    def _send_to_kafka(self, topic, key, data):
        """Helper to send data to Kafka."""
        sentry_sdk.add_breadcrumb(
            message=f"Sending data to Kafka: topic={topic}, key={key}",
            data={"data_preview": data if self.config.is_beta else "Data suppressed in production"},
            level="info",
        )
        self.producer.send(
            topic=topic,
            value=ujson.dumps(data).encode('utf-8'),
            key=key.encode('utf-8'),
            timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000),
        )
        self.message_count += 1

    def _handle_exception(self, message, exception):
        """Handle exceptions by logging to Sentry and console."""
        sentry_sdk.capture_exception(exception)
        print(f"{message}: {exception}")


async def main(tracker_class, config_type="bot_clan", loop_interval=60, is_tracking_allowed=None):
    """
    Main function for generic tracking with conditional execution.

    :param tracker_class: The tracker class to instantiate (e.g., ClanTracker, RaidTracker).
    :param config_type: The configuration type (default: "bot_clan").
    :param loop_interval: The interval in seconds between tracking loops.
    :param is_tracking_allowed: A function that returns True if tracking should run, False otherwise.
    """
    config = Config(config_type=config_type)
    await config.initialize()

    sentry_sdk.init(
        dsn=config.sentry_dsn,
        traces_sample_rate=1.0,
        integrations=[AsyncioIntegration()],
        profiles_sample_rate=1.0,
        environment="production" if not config.is_beta else "beta",
        before_send=sentry_filter,
    )

    producer = MockKafkaProducer() if config.is_beta else KafkaProducer(bootstrap_servers=["85.10.200.219:9092"])

    tracker = tracker_class(config, producer=producer)
    await tracker.initialize()

    try:
        while True:
            if is_tracking_allowed is None or is_tracking_allowed():
                clan_tags = await tracker.db_client.clans_db.distinct("tag")
                start_time = pend.now(tz=pend.UTC)
                await tracker.track(clan_tags)
                elapsed_time = pend.now(tz=pend.UTC) - start_time
                print(f"Tracked all {len(clan_tags)} clans in {elapsed_time.in_seconds()} seconds.")
                print(
                    f"Total messages sent: {tracker.message_count} which is {tracker.message_count / elapsed_time.in_seconds()} messages per second."
                )
            else:
                print("Tracking is not allowed at this time. Sleeping until the next interval.")
            await asyncio.sleep(loop_interval)
    except KeyboardInterrupt:
        print("Tracking interrupted by user.")
    finally:
        await tracker.coc_client.close()
        print("Tracking completed.")
