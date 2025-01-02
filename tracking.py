import asyncio

import sentry_sdk
from sentry_sdk.integrations.asyncio import AsyncioIntegration

from bot.dev.kafka_mock import MockKafkaProducer
from utility.config import Config
from asyncio_throttle import Throttler
from utility.classes import MongoDatabase
import coc
import pendulum as pend
from redis import asyncio as redis
from kafka import KafkaProducer
from loguru import logger
import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import ujson

from utility.utils import sentry_filter


class Tracking():
    def __init__(self, config, producer=None, max_concurrent_requests=1000, batch_size=500, throttle_speed=1000):
        self.config = config
        self.producer = producer or KafkaProducer(bootstrap_servers=["85.10.200.219:9092"])
        self.db_client = None
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.message_count = 0
        self.iterations = 0
        self.batch_size = batch_size
        self.throttler = Throttler(throttle_speed)
        self.coc_client = None
        self.redis = None
        self.logger = logger
        self.http_session = None
        self.scheduler = None
        self.kafka = None

    async def initialize(self):
        """Initialise the tracker."""
        await self._initialize_config()
        await self._initialize_connections()

    async def _initialize_config(self):
        """Initialise the configuration."""
        self.db_client = MongoDatabase(
            stats_db_connection=self.config.stats_mongodb,
            static_db_connection=self.config.static_mongodb,
        )
        self.coc_client = self.config.coc_client
        if not self.coc_client:
            raise RuntimeError("CoC client is not initialized in config.")
        self.keys = self.config.keys
        self.redis = redis.Redis(
            host=self.config.redis_ip, port=6379, db=0,
            password=self.config.redis_pw, decode_responses=False,
            max_connections=50, health_check_interval=10,
            socket_connect_timeout=5, retry_on_timeout=True,
            socket_keepalive=True,
        )

    async def _initialize_connections(self):
        """Initialise the connections."""
        connector = aiohttp.TCPConnector(limit=1200, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=1800)
        self.http_session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        if self.config.is_main:
            self.kafka = KafkaProducer(bootstrap_servers=["85.10.200.219:9092"], api_version=(3, 6, 0))
        else:
            self.kafka = MockKafkaProducer()  # Mock producer for beta

        self.scheduler = AsyncIOScheduler(timezone=pend.UTC)

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

    @staticmethod
    def _handle_exception(message, exception):
        """Handle exceptions by logging to Sentry and console."""
        sentry_sdk.capture_exception(exception)
        print(f"{message}: {exception}")

    @staticmethod
    def gen_raid_date():
        now = pend.now(tz=pend.UTC)
        current_dayofweek = now.day_of_week  # Monday = 0, Sunday = 6
        if (
                (current_dayofweek == 4 and now.hour >= 7)  # Friday after 7 AM UTC
                or (current_dayofweek == 5)  # Saturday
                or (current_dayofweek == 6)  # Sunday
                or (current_dayofweek == 0 and now.hour < 7)  # Monday before 7 AM UTC
        ):
            raid_date = now.subtract(days=(current_dayofweek - 4 if current_dayofweek >= 4 else 0)).date()
        else:
            forward = 4 - current_dayofweek  # Days until next Friday
            raid_date = now.add(days=forward).date()
        return str(raid_date)

    @staticmethod
    def gen_season_date():
        end = coc.utils.get_season_end().astimezone(pend.UTC)
        month = f"{end.month:02}"
        return f"{end.year}-{month}"

    @staticmethod
    def gen_legend_date():
        now = pend.now(tz=pend.UTC)
        date = now.subtract(days=1).date() if now.hour < 5 else now.date()
        return str(date)

    @staticmethod
    def gen_games_season():
        now = pend.now(tz=pend.UTC)
        month = f"{now.month:02}"  # Ensure two-digit month
        return f"{now.year}-{month}"

    @staticmethod
    def is_raids():
        now = pend.now(tz=pend.UTC)
        current_dayofweek = now.day_of_week  # Monday = 0, Sunday = 6
        return (
                (current_dayofweek == 4 and now.hour >= 7)  # Friday after 7 AM UTC
                or (current_dayofweek == 5)  # Saturday
                or (current_dayofweek == 6)  # Sunday
                or (current_dayofweek == 0 and now.hour < 9)  # Monday before 9 AM UTC
        )


async def main(tracker_class, config_type="bot_clan", loop_interval=60, is_tracking_allowed=None):
    """Main function for generic tracking."""
    # Initialize tracker
    config = Config(config_type=config_type)
    await config.initialize()
    tracker = tracker_class(config, producer=config.get_producer())
    await tracker.initialize()

    # Initialize Sentry for error tracking
    sentry_sdk.init(
        dsn=config.sentry_dsn,
        traces_sample_rate=1.0,
        integrations=[AsyncioIntegration()],
        profiles_sample_rate=1.0,
        environment="production" if not config.is_beta else "beta",
        before_send=sentry_filter,
    )

    # Run tracking loop
    try:
        async with tracker.http_session:
            while True:
                if is_tracking_allowed is None or is_tracking_allowed():
                    clan_tags = await tracker.db_client.clans_db.distinct("tag")
                    start_time = pend.now(tz=pend.UTC)
                    await tracker.track(clan_tags)
                    elapsed_time = pend.now(tz=pend.UTC) - start_time
                    tracker.logger.info(
                        f"Tracked {len(clan_tags)} clans in {elapsed_time.in_seconds()} seconds. "
                        f"Messages sent: {tracker.message_count} "
                        f"({tracker.message_count / elapsed_time.in_seconds()} msg/s)."
                    )
                else:
                    tracker.logger.info("Tracking not allowed. Sleeping until the next interval.")
                await asyncio.sleep(loop_interval)
    except KeyboardInterrupt:
        tracker.logger.info("Tracking interrupted by user.")
    finally:
        await tracker.coc_client.close()
        tracker.logger.info("Tracking completed.")
