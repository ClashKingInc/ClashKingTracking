from utility.config import Config
from dataclasses import dataclass

@dataclass
class BotClanTrackingConfig(Config):
    min_coc_email = 41
    max_coc_email = 45
    redis_max_connections = 250