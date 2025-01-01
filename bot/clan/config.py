from dataclasses import dataclass, field
from utility.config import create_config, master_api_config


@dataclass
class BotClanTrackingConfig:
    # General settings
    static_mongodb: str
    stats_mongodb: str
    coc_email: str
    coc_password: str
    sentry_dsn: str
    bot_token: str
    tracking_type: str
    redis_ip: str
    redis_pw: str
    reddit_user_secret: str
    reddit_user_password: str
    meili_pw: str
    is_beta: bool
    is_custom: bool
    is_main: bool
    webhook_url: str

    # Specific settings
    min_coc_email: int = field(default=4)
    max_coc_email: int = field(default=6)
    redis_max_connections: int = field(default=250)

    @classmethod
    def from_remote_settings(cls):
        """Initialize BotClanTrackingConfig from remote settings."""
        base_config = create_config()

        # Override min_coc_email and max_coc_email if necessary
        min_email, max_email = (4, 6)  # Default for beta
        if not base_config.is_beta:
            min_email, max_email = master_api_config.get('bot_clan')

        # Merge base_config with dynamic values
        config_dict = {
            **vars(base_config),
            "min_coc_email": min_email,
            "max_coc_email": max_email,
        }

        return cls(**config_dict)
