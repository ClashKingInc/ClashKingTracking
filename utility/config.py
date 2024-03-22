from os import getenv
from dotenv import load_dotenv
from dataclasses import dataclass
load_dotenv()

@dataclass
class Config:
    coc_email = getenv("COC_EMAIL")
    coc_password = getenv("COC_PASSWORD")

    static_mongodb = getenv("STATIC_MONGODB")
    stats_mongodb = getenv("STATS_MONGODB")

    redis_ip = getenv("REDIS_IP")
    redis_pw = getenv("REDIS_PW")

    #tracking_type = getenv("TYPE")
    tracking_type = "GLOBALPLAYER"








