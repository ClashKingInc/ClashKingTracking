from utility.config import Config, master_api_config
from dataclasses import dataclass

@dataclass
class BotClanTrackingConfig(Config):
    min_coc_email = master_api_config.get("bot_clan")[0]
    max_coc_email = master_api_config.get("bot_clan")[1]
    redis_max_connections = 250

