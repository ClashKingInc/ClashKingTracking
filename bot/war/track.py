import asyncio

import pendulum as pend
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from config import BotWarTrackingConfig
from kafka import KafkaProducer
from utils import clan_war_track

from utility.classes import MongoDatabase
from utility.utils import initialize_coc_client


async def main():
    """Main function for war tracking."""
    config = BotWarTrackingConfig()
    scheduler = AsyncIOScheduler(timezone=pend.UTC)
    scheduler.start()

    producer = KafkaProducer(bootstrap_servers=['85.10.200.219:9092'])
    db_client = MongoDatabase(
        stats_db_connection=config.stats_mongodb,
        static_db_connection=config.static_mongodb,
    )
    coc_client = await initialize_coc_client(config)

    while True:
        try:
            # Fetch all clan tags from the database
            clan_tags = await db_client.clans_db.distinct('tag')
            for clan_tag in clan_tags:
                await clan_war_track(
                    clan_tag, db_client, coc_client, producer, scheduler
                )
            print('Finished war tracking for all clans.')
        except Exception as e:
            print(f'Error in war tracking: {e}')
        await asyncio.sleep(300)  # Adjust interval as needed


asyncio.run(main())
