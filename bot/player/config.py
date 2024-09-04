from dataclasses import dataclass

from utility.config import Config, master_api_config


@dataclass
class BotPlayerTrackingConfig(Config):
    min_coc_email = master_api_config.get('bot_player')[0]
    max_coc_email = master_api_config.get('bot_player')[1]
    redis_max_connections = 2500

    secondary_loop_change = 15
    tertiary_loop_change = 150
    max_tag_split = 50_000
