from dataclasses import dataclass
from os import getenv

from dotenv import load_dotenv
import requests

load_dotenv()


class Config:
    def __init__(self, remote_settings: dict):
        # MongoDB Credentials
        self.static_mongodb: str = remote_settings.get('static_db')
        self.stats_mongodb: str = remote_settings.get('stats_db')

        # COC API Credentials
        self.coc_email: str = remote_settings.get('coc_email')
        self.coc_password: str = remote_settings.get('coc_password')

        self.bot_token: str = ''
        self.tracking_type = "GLOBALSCHEDULED"

        # Sentry DSN
        self.sentry_dsn: str = remote_settings.get('sentry_dsn_tracking')

        # Redis Credentials
        self.redis_ip = remote_settings.get('redis_ip')
        self.redis_pw = remote_settings.get('redis_pw')

        # Reddit Credentials
        self.reddit_user_secret = remote_settings.get('reddit_secret')
        self.reddit_user_password = remote_settings.get('reddit_pw')

        # MeiliSearch Credentials
        self.meili_pw = remote_settings.get('meili_pw')

        # Type of environment
        self.is_beta = remote_settings.get('is_beta')
        self.is_custom = remote_settings.get('is_custom')
        self.is_main = remote_settings.get('is_main')

        # Webhook URL
        self.webhook_url = remote_settings.get('webhook_url')


def create_config() -> Config:
    BOT_TOKEN = getenv('BOT_TOKEN')
    bot_config_url = 'https://api.clashking.xyz/bot/config'
    bot_config = requests.get(bot_config_url, timeout=5, headers={'bot-token': BOT_TOKEN}).json()
    config = Config(remote_settings=bot_config)
    config.bot_token = BOT_TOKEN
    return config


master_api_config = {
    'bot_clan': (41, 45),
    'bot_player': (11, 15),
    'bot_legends': (16, 20),
    'global_clan_find': (21, 25),
    'global_clan_verify': (46, 50),
    'global_scheduled': (26, 30),
    'global_war': (31, 38),
    'global_war_store': (39, 40),
}
