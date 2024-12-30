import asyncio
from kafka import KafkaProducer
from utility.classes import MongoDatabase
from .config import BotRaidsTrackingConfig
from utility.utils import initialize_coc_client
from utils import raid_weekend_track

async def main():
    """Main function for raid tracking."""
    config = BotRaidsTrackingConfig()
    producer = KafkaProducer(bootstrap_servers=["85.10.200.219:9092"])
    db_client = MongoDatabase(
        stats_db_connection=config.stats_mongodb,
        static_db_connection=config.static_mongodb,
    )
    coc_client = await initialize_coc_client(config)

    while True:
        try:
            # Fetch all clan tags from the database
            clan_tags = await db_client.clans_db.distinct("tag")
            await raid_weekend_track(clan_tags, db_client, coc_client, producer)
            print("Finished raid tracking for all clans.")
        except Exception as e:
            print(f"Error in raid tracking: {e}")
        await asyncio.sleep(600)  # Adjust interval as needed

asyncio.run(main())
