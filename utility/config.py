from collections import deque
from enum import Enum
from os import getenv

import coc
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from redis import Redis

from kafka_mock import MockKafkaProducer
from utility.mongo import MongoDatabase
from utility.keycreation import create_keys

# Load environment variables from .env file
load_dotenv()

# Configuration mapping for different types
MASTER_API_CONFIG = {
    "bot_clan": (41, 42),
    "bot_raids": (43, 43),
    "bot_war": (44, 45),
    "bot_player": (11, 15),
    "bot_legends": (16, 20),
    "global_clan_find": (21, 25),
    "global_clan_verify": (46, 50),
    "global_scheduled": (26, 30),
    "global_war": (31, 38),
    "global_war_store": (39, 40),
}


class TrackingType(Enum):
    BOT_CLAN = "bot_clan"
    BOT_RAIDS = "bot_raids"
    BOT_WAR = "bot_war"
    BOT_PLAYER = "bot_player"
    BOT_LEGENDS = "bot_legends"
    GLOBAL_CLAN_FIND = "global_clan_find"
    GLOBAL_CLAN_VERIFY = "global_clan_verify"
    GLOBAL_SCHEDULED = "global_scheduled"
    GLOBAL_WAR = "global_war"
    GLOBAL_WAR_STORE = "global_war_store"
    GIVEAWAY = "giveaway"
    REDDIT = "reddit"
    WEBSOCKET = "websocket"

    def __str__(self):
        return self.value


class Config:
    def __init__(self, config_type: TrackingType):
        """
        Initialize the Config object by fetching remote settings and setting up attributes.

        :param config_type: The type of configuration to load (e.g., 'bot_clan')
        """
        self.type = str(config_type)

        # Load BOT_TOKEN from environment
        self.bot_token = getenv("BOT_TOKEN", "")
        if not self.bot_token:
            raise ValueError("BOT_TOKEN is not set in the environment variables.")

        # Fetch remote settings from the API
        self._fetch_remote_settings()

        # Initialize other attributes
        self.coc_client = coc.Client()
        self.keys = deque()

    def _fetch_remote_settings(self):
        """
        Fetch remote configuration settings from the API and assign them to instance attributes.
        """
        bot_config_url = "https://api.clashking.xyz/bot/config"
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

        # Determine the account range based on config_type
        self.__beta_range = (7, 10)
        self.account_range = MASTER_API_CONFIG.get(self.type, (0, 0)) if not self.is_beta else self.__beta_range
        self.min_coc_email, self.max_coc_email = self.account_range

    async def initialize(self):
        """
        Asynchronously initialize the CoC client by generating keys and logging in.
        """
        if not self.coc_email or not self.coc_password:
            raise ValueError("CoC email or password is not set in the configuration.")

        # Generate list of emails based on the account range
        keys = [""]
        if self.min_coc_email:
            emails = [self.coc_email.format(x=x) for x in range(self.min_coc_email, self.max_coc_email + 1)]

            # Generate matching passwords
            passwords = [self.coc_password] * len(emails)

            # Create keys using the provided utility function
            try:
                keys = await create_keys(emails, passwords, as_list=True)
            except Exception as e:
                raise RuntimeError(f"Failed to create keys: {e}")

        # Initialize the CoC client with desired parameters
        self.coc_client = coc.Client(throttle_limit=30, cache_max_size=0, raw_attribute=True)

        # Log in to the CoC client using the generated keys
        try:
            await self.coc_client.login_with_tokens(*keys)
        except Exception as e:
            raise ConnectionError(f"Failed to log in to CoC client: {e}")

        # Store the keys in a deque for future use
        self.keys = deque(keys)

    def get_kafka_producer(self):
        if self.is_main:
            return KafkaProducer(bootstrap_servers=[self.kafka_host], api_version=(3, 6, 0))
        return MockKafkaProducer()

    def get_mongo_database(self, sync: bool = True):
        return MongoDatabase(
            stats_db_connection=self.stats_mongodb, static_db_connection=self.static_mongodb, sync=sync
        )

    def get_redis_client(self):
        return Redis(
            host=self.redis_ip,
            port=6379,
            db=0,
            password=self.redis_pw,
            decode_responses=False,
            max_connections=50,
            health_check_interval=10,
            socket_connect_timeout=5,
            retry_on_timeout=True,
            socket_keepalive=True,
        )
