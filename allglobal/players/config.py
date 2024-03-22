from utility.config import Config
from dataclasses import dataclass


@dataclass
class GlobalPlayerTrackingConfig(Config):
    min_coc_email = 11
    max_coc_email = 18
    redis_max_connections = 2500

    secondary_loop_change = 15
    tertiary_loop_change = 150
    max_tag_split = 50_000