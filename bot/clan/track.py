import asyncio
import coc
import pendulum as pend

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from .config import BotClanTrackingConfig
from loguru import logger
from kafka import KafkaProducer
from utility.classes import MongoDatabase
from utility.keycreation import create_keys
from utility.utils import is_raids
from .utils import clan_war_track, raid_weekend_track, clan_track


async def main():
    scheduler = AsyncIOScheduler(timezone=pend.UTC)
    scheduler.start()

    config = BotClanTrackingConfig()

    producer = KafkaProducer(bootstrap_servers=["85.10.200.219:9092"], api_version=(3, 6, 0))
    db_client = MongoDatabase(stats_db_connection=config.stats_mongodb, static_db_connection=config.static_mongodb)

    keys: list = await create_keys([config.coc_email.format(x=x) for x in range(config.min_coc_email, config.max_coc_email + 1)], [config.coc_password] * config.max_coc_email, as_list=True)
    coc_client = coc.Client(raw_attribute=True, key_count=10, throttle_limit=100)
    await coc_client.login_with_tokens(*keys)

    while True:
        clan_tags = await db_client.clans_db.distinct("tag")
        tasks = []
        for clan_tag in clan_tags:
            tasks.append(clan_war_track(clan_tag=clan_tag, db_client=db_client, coc_client=coc_client, producer=producer, scheduler=scheduler))
        await asyncio.gather(*tasks)

        logger.info(f"Finished War Tracking")

        #if war state is "warEnded", send war_end event

        if is_raids():
            await raid_weekend_track(clan_tags=clan_tags, db_client=db_client, coc_client=coc_client, producer=producer)
            logger.info(f"Finished Raid Tracking")

        tasks = []
        for clan_tag in clan_tags:
            tasks.append(clan_track(clan_tag=clan_tag, coc_client=coc_client, producer=producer))
        await asyncio.gather(*tasks)
        logger.info(f"Finished Clan Tracking")







