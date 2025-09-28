from collections import deque
from os import getenv

import coc
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from redis import Redis
from redis.asyncio import Redis as AsyncRedis

from utility.kafka_mock import MockKafkaProducer
from utility.mongo import MongoDatabase

# Load environment variables from .env file
load_dotenv()

class Config:
    def __init__(self):
        """
        Initialize the Config object by fetching remote settings and setting up attributes.

        """
        # Load BOT_TOKEN from environment
        self.bot_token = getenv("BOT_TOKEN", "")
        if not self.bot_token:
            raise ValueError("BOT_TOKEN is not set in the environment variables.")

        # Fetch remote settings from the API
        self._fetch_remote_settings()

        # Initialize other attributes
        self.coc_client = coc.Client()

    def _fetch_remote_settings(self):
        """
        Fetch remote configuration settings from the API and assign them to instance attributes.
        """
        bot_config_url = "https://api.clashk.ing/bot/config"
        try:
            response = requests.get(bot_config_url, timeout=5, headers={"bot-token": self.bot_token})
            response.raise_for_status()
        except requests.RequestException as e:
            raise ConnectionError(f"Failed to fetch remote settings: {e}")

        remote_settings = response.json()

        # Assign attributes from remote settings
        self.static_mongodb = remote_settings.get("static_db")
        self.stats_mongodb = remote_settings.get("stats_db")
        self.coc_email = remote_settings.get("coc_email")
        self.coc_password = remote_settings.get("coc_password")
        self.sentry_dsn = remote_settings.get("sentry_dsn_tracking")
        self.redis_ip = remote_settings.get("redis_ip")
        self.redis_pw = remote_settings.get("redis_pw")
        self.reddit_user_secret = remote_settings.get("reddit_secret")
        self.reddit_user_password = remote_settings.get("reddit_pw")
        self.meili_pw = remote_settings.get("meili_pw")
        self.is_beta = remote_settings.get("is_beta", False)
        self.is_main = remote_settings.get("is_main", False)
        self.webhook_url = remote_settings.get("webhook_url")
        self.kafka_host = remote_settings.get("kafka_connection")
        self.kafka_user = remote_settings.get("kafka_connection")
        self.kafka_password = remote_settings.get("kafka_password")
        self.proxy_url = remote_settings.get("local_coc_proxy_url")

    async def initialize(self):
        """
        Asynchronously initialize the CoC client by generating keys and logging in.
        """

        # Initialize the CoC client with desired parameters
        self.coc_client = coc.Client(
            base_url=self.proxy_url,
            throttle_limit=500,
            cache_max_size=0,
            raw_attribute=True,
            load_game_data=coc.LoadGameData(never=True)
        )

        # Log in to the CoC client using the generated keys
        await self.coc_client.login_with_tokens("")

        # Store the keys in a deque for future use

    def get_kafka_producer(self):
        if self.is_main:
            return KafkaProducer(bootstrap_servers=self.kafka_host, api_version=(3, 6, 0))
        return MockKafkaProducer()

    def get_mongo_database(self, sync: bool = True):
        return MongoDatabase(
            stats_db_connection=self.stats_mongodb, static_db_connection=self.static_mongodb, sync=sync
        )

    async def get_redis_client(self, decode_responses: bool, sync: bool = True):
        cls = Redis
        if not sync:
            cls = AsyncRedis
        return cls(
            host=self.redis_ip,
            port=6379,
            db=0,
            password=self.redis_pw,
            decode_responses=decode_responses,
            max_connections=50,
            health_check_interval=10,
            socket_connect_timeout=5,
            retry_on_timeout=True,
            socket_keepalive=True,
        )
