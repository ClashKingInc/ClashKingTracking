from os import getenv
import requests
import coc
from dotenv import load_dotenv
from collections import deque

from kafka import KafkaProducer

from bot.dev.kafka_mock import MockKafkaProducer
from .keycreation import create_keys

# Load environment variables from .env file
load_dotenv()

# Configuration mapping for different types
MASTER_API_CONFIG = {
    'bot_clan': (41, 45),
    'bot_player': (11, 15),
    'bot_legends': (16, 20),
    'global_clan_find': (21, 25),
    'global_clan_verify': (46, 50),
    'global_scheduled': (26, 30),
    'global_war': (31, 38),
    'global_war_store': (39, 40),
}


class Config():
    def __init__(self, config_type: str):
        """
        Initialize the Config object by fetching remote settings and setting up attributes.

        :param config_type: The type of configuration to load (e.g., 'bot_clan')
        """
        self.type = config_type

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
            response = requests.get(
                bot_config_url,
                timeout=5,
                headers={"bot-token": self.bot_token},
            )
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

        # Determine the account range based on config_type
        self.__beta_range = (4, 6)
        self.account_range = MASTER_API_CONFIG.get(self.type, (0, 0)) if not self.is_beta else self.__beta_range
        self.min_coc_email, self.max_coc_email = self.account_range

    async def initialize(self):
        """
        Asynchronously initialize the CoC client by generating keys and logging in.
        """
        if not self.coc_email or not self.coc_password:
            raise ValueError("CoC email or password is not set in the configuration.")

        # Generate list of emails based on the account range
        emails = [
            self.coc_email.format(x=x)
            for x in range(self.min_coc_email, self.max_coc_email + 1)
        ]

        # Generate matching passwords
        passwords = [self.coc_password] * len(emails)

        # Create keys using the provided utility function
        try:
            keys = await create_keys(emails, passwords, as_list=True)
        except Exception as e:
            raise RuntimeError(f"Failed to create keys: {e}")

        # Initialize the CoC client with desired parameters
        self.coc_client = coc.Client(
            throttle_limit=30,
            cache_max_size=0,
            raw_attribute=True
        )

        # Log in to the CoC client using the generated keys
        try:
            await self.coc_client.login_with_tokens(*keys)
        except Exception as e:
            raise ConnectionError(f"Failed to log in to CoC client: {e}")

        # Store the keys in a deque for future use
        self.keys = deque(keys)

    def get_producer(self):
        """Initialize the Kafka producer based on the environment."""
        if self.is_main:
            return KafkaProducer(bootstrap_servers=["85.10.200.219:9092"], api_version=(3, 6, 0))
        else:
            print("We are in beta mode, using MockKafkaProducer.")
            return MockKafkaProducer()
