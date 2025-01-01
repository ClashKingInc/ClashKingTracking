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


class Tracking():
    def __init__(self):
        self.type: str = None
        self.throttler: Throttler = Throttler(1000)
        self.iterations = 0
        #maybe add different stats, and then a function that print them out or smthn


    def initialize(self):
        print(self.type)
        self.config = Config(config_type=self.type)
        self.config.initialize()

        self.db_client = MongoDatabase(
            stats_db_connection=self.config.stats_mongodb,
            static_db_connection=self.config.static_mongodb,
        )
        self.coc_client = self.config.coc_client
        self.keys = self.config.keys
        self.redis = redis.Redis(host=self.config.redis_ip, port=6379, db=0,
                                 password=self.config.redis_pw, decode_responses=False,
                                 max_connections=50,
                                 health_check_interval=10, socket_connect_timeout=5,
                                 retry_on_timeout=True, socket_keepalive=True)
        #insert fake producer from desti
        self.kafka = KafkaProducer(bootstrap_servers=["85.10.200.219:9092"], api_version=(3, 6, 0)) if self.config.is_main else None
        self.logger = logger

        connector = aiohttp.TCPConnector(limit=1200, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=1800)
        self.http_session = aiohttp.ClientSession(connector=connector, timeout=timeout)

        self.scheduler = AsyncIOScheduler(timezone=pend.UTC)



    def gen_raid_date(self):
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

    def gen_season_date(self):
        end = coc.utils.get_season_end().astimezone(pend.UTC)
        month = f"{end.month:02}"
        return f"{end.year}-{month}"

    def gen_legend_date(self):
        now = pend.now(tz=pend.UTC)
        date = now.subtract(days=1).date() if now.hour < 5 else now.date()
        return str(date)

    def gen_games_season(self):
        now = pend.now(tz=pend.UTC)
        month = f"{now.month:02}"  # Ensure two-digit month
        return f"{now.year}-{month}"

    def is_raids(self):
        now = pend.now(tz=pend.UTC)
        current_dayofweek = now.day_of_week  # Monday = 0, Sunday = 6
        return (
                (current_dayofweek == 4 and now.hour >= 7)  # Friday after 7 AM UTC
                or (current_dayofweek == 5)  # Saturday
                or (current_dayofweek == 6)  # Sunday
                or (current_dayofweek == 0 and now.hour < 9)  # Monday before 9 AM UTC
        )