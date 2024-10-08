from dataclasses import dataclass
from os import getenv

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    coc_email = getenv('COC_EMAIL')
    coc_password = getenv('COC_PASSWORD')

    static_mongodb = getenv('STATIC_MONGODB')
    stats_mongodb = getenv('STATS_MONGODB')

    redis_ip = getenv('REDIS_IP')
    redis_pw = getenv('REDIS_PW')

    tracking_type = getenv('TYPE')

    meili_pw = getenv('MEILI_PW')

    webhook_url = getenv('WEBHOOK_URL')


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
