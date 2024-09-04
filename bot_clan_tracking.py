import asyncio
from collections import deque

import coc
import motor.motor_asyncio
from loguru import logger
from redis import asyncio as redis

from bot.clan.track import main
from utility.config import BotClanTrackingConfig
from utility.http import HTTPClient
from utility.keycreation import create_keys

if __name__ == '__main__':
    config = BotClanTrackingConfig()
    loop = asyncio.get_event_loop()

    stats_mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
        config.stats_mongodb
    )
    static_mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
        config.static_mongodb
    )

    connection_pool = redis.connection.ConnectionPool(
        max_connections=config.redis_max_connections
    )
    redis_host = redis.Redis(
        host=config.redis_ip,
        port=6379,
        db=5,
        password=config.redis_pw,
        decode_responses=False,
        connection_pool=connection_pool,
    )
    keys = create_keys(
        [
            config.coc_email.format(x=x)
            for x in range(config.min_coc_email, config.max_coc_email + 1)
        ],
        [config.coc_password] * config.max_coc_email,
    )
    logger.info(f'{len(keys)} keys created')

    # set config attributes
    config.keys = deque(keys)
    config.stats_mongodb = stats_mongo_client
    config.static_mongodb = static_mongo_client
    config.redis_client = redis_host

    config.coc_client = coc.Client(key_count=5, raw_attribute=True)
    config.http_client = HTTPClient()

    # set db's
    config.player_stats_db = stats_mongo_client.new_looper.player_stats
    config.clan_stats_db = stats_mongo_client.new_looper.clan_stats
    config.clan_war_db = stats_mongo_client.looper.clan_war

    config.clan_db = static_mongo_client.usafam.clans
    config.player_search_db = static_mongo_client.usafam.player_search

    loop.create_task(main(config=config))
    loop.run_forever()
