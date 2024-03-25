from utility.config import Config, master_api_config
from dataclasses import dataclass


@dataclass
class GlobalScheduledConfig(Config):
    min_coc_email = master_api_config.get("global_scheduled")[0]
    max_coc_email = master_api_config.get("global_scheduled")[1]
    redis_max_connections = 2500

    secondary_loop_change = 15
    tertiary_loop_change = 150
    max_tag_split = 50_000