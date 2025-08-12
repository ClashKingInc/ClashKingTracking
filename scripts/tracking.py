import asyncio
from collections import defaultdict, deque

import aiohttp
import coc
import orjson
import pendulum as pend
import sentry_sdk
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from asyncio_throttle import Throttler
from kafka import KafkaProducer
from loguru import logger
from redis import Redis
from redis.asyncio import Redis as AsyncRedis

from utility.mongo import MongoDatabase
from utility.config import Config, TrackingType
from utility.health import run_health_check_server


class Tracking:
    def __init__(self, batch_size: int = 500, tracker_type: TrackingType = ...):
        self.is_maintenance = False
        self.batch_size = batch_size
        self.max_concurrent_requests = 1000
        self.throttle_speed = self.max_concurrent_requests

        self.tracker_type: TrackingType = tracker_type

        self.config = Config(self.tracker_type)
        self.mongo: MongoDatabase = ...
        self.async_mongo: MongoDatabase = ...

        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        self.message_count = 0
        self.iterations = 0
        self.throttler = Throttler(self.throttle_speed)

        self.coc_client: coc.Client = ...
        self.redis_raw: Redis = ...
        self.redis_decoded: Redis = ...
        self.aredis_decoded: AsyncRedis = ...

        self.logger = logger
        self.http_session = None
        self.scheduler: AsyncIOScheduler = ...
        self.kafka: KafkaProducer = ...
        self.type = self.tracker_type
        self.max_stats_size = 10_000
        self.request_stats = defaultdict(lambda: deque(maxlen=self.max_stats_size))
        self.keys = deque()

        self._api_requests_made = 0
        self._cycle_count = 0
        self._last_run = pend.now(tz=pend.UTC)


    async def initialize(self):
        """Initialise the tracker with dependencies."""
        await self.config.initialize()
        await run_health_check_server()

        self.mongo = self.config.get_mongo_database()
        self.async_mongo = self.config.get_mongo_database(sync=False)
        self.redis_raw = await self.config.get_redis_client(decode_responses=False)
        self.redis_decoded = await self.config.get_redis_client(decode_responses=True)
        self.aredis_decoded = await self.config.get_redis_client(decode_responses=True, sync=False)

        self.coc_client = self.config.coc_client
        self.keys = self.config.keys
        self.kafka = self.config.get_kafka_producer()

        connector = aiohttp.TCPConnector(limit=1200, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=1800)
        self.http_session = aiohttp.ClientSession(connector=connector, timeout=timeout, json_serialize=orjson.loads)

        self.scheduler = AsyncIOScheduler(timezone=pend.UTC)

    async def track(self, items):
        """Track items in batches."""
        self.message_count = 0  # Reset message count
        for i in range(0, len(items), self.batch_size):
            batch = items[i : i + self.batch_size]
            print(f"Processing batch {i // self.batch_size + 1} of {len(items) // self.batch_size + 1}.")
            await self._track_batch(batch)

        sentry_sdk.add_breadcrumb(message="Finished tracking all clans.", level="info")
        print("Finished tracking all clans.")

    async def fetch(
        self, url: str, tag: str | None = None, json: bool = False
    ) -> tuple[dict | bytes, str] | dict | bytes | aiohttp.ClientResponse:
        async with self.throttler:
            self.keys.rotate(1)
            self.request_stats["all"].append({"time": pend.now(tz=pend.UTC).timestamp()})
            self._api_requests_made += 1
            async with self.http_session.get(url, headers={"Authorization": f"Bearer {self.keys[0]}"}) as response:
                if response.status != 200 or json:
                    data = await response.json()
                else:
                    data = await response.read()
                if response.status == 200:
                    if tag:
                        return data, tag
                    return data
                elif response.status == 400:
                    raise coc.InvalidArgument(400, data)
                elif response.status == 403:
                    raise coc.Forbidden(403, data)
                elif response.status == 404:
                    err = coc.NotFound(404, data)
                    if tag:
                        err.add_note(tag)
                    raise err
                elif response.status == 503:
                    raise coc.Maintenance(503, data)

    def _submit_stats(self):
        now = pend.now(tz=pend.UTC)
        time_since_last_run = (now - self._last_run).total_seconds()
        result = self.mongo.tracking_stats.update_one(
            filter={"type": str(self.tracker_type)},
            update={
            "$set": {
                "last_cycle": now.int_timestamp,
                "current_cycle": self._cycle_count,
                "number_tracked": self._api_requests_made,
            },
            "$inc": {
                "total_cycles_run": 1,
                "total_api_requests_made": self._api_requests_made
            },
            "$push": {
                "cycles": {"$each": [time_since_last_run], "$slice": -1000}
            }
        }, upsert=True)
        self._last_run = now
        self._cycle_count += 1
        self._api_requests_made = 0

    async def _check_maintenance(self) -> float:
        start = pend.now(tz=pend.UTC)
        is_maintenance = False
        while True:
            try:
                await self.fetch(url="https://api.clashofclans.com/v1/goldpass/seasons/current")
                break
            except coc.ClashOfClansException:
                if not is_maintenance:
                    json_data = {"maintenance_status": "start", "maintenance_duration": 0}
                    self._send_to_kafka("maintenance", json_data, None)
                    is_maintenance = True
                self.logger.info("API in maintenance, retrying in 15s…")
                await asyncio.sleep(15)

        downtime = int((pend.now(tz=pend.UTC) - start).total_seconds())
        return downtime

    async def _run_tasks(self, tasks: list, return_exceptions: bool, wrapped: bool):
        async def wrap(coro):
            async with self.semaphore:
                return await coro

        if wrapped:
            tasks = [wrap(task) for task in tasks]

        results = await asyncio.gather(*tasks, return_exceptions=return_exceptions)
        return results

    async def _batch_tasks(self, tasks: list):
        """Track a batch of items."""
        full_results = []
        for i in range(0, len(tasks), self.batch_size):
            batch = tasks[i : i + self.batch_size]
            results = await self._run_tasks(tasks=batch, return_exceptions=True, wrapped=True)
            full_results.extend(results)
        logger.info(f"Finished {len(tasks)} tracking tasks")
        return full_results

    def _split_into_batch(self, items: list, batch_size: int = None):
        batch_size = batch_size or self.batch_size
        return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]

    def _chunk_into_n(self, lst: list, n: int):
        length = len(lst)
        if n <= 0:
            raise ValueError("n must be positive")
        # base size and remainder
        k, m = divmod(length, n)
        chunks = []
        start = 0
        for i in range(n):
            # each of the first m chunks gets an extra item
            size = k + (1 if i < m else 0)
            chunks.append(lst[start : start + size])
            start += size
        return chunks

    async def _track_item(self, item):
        """Override this method in child classes."""
        raise NotImplementedError("This method should be overridden in child classes.")

    def _send_to_kafka(self, topic: str, data: dict, key: str | None):
        """Helper to send data to Kafka."""
        """sentry_sdk.add_breadcrumb(
            message=f'Sending data to Kafka: topic={topic}, key={key}',
            data={
                'data_preview': data
                if self.config.is_beta
                else 'Data suppressed in production'
            },
            level='info',
        )"""
        self.kafka.send(
            topic=topic,
            value=orjson.dumps(data),
            key=key.encode("utf-8") if key else None,
            timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000),
        )
        self.message_count += 1

    @staticmethod
    def _handle_exception(message, exception):
        """Handle exceptions by logging to Sentry and console."""
        sentry_sdk.capture_exception(exception)
        print(f"{message}: {exception}")
