import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime

from aiokafka import AIOKafkaProducer
from loguru import logger

from bot.giveaway.utils import produce_giveaway_event
from utility.config import Config
from utility.classes import MongoDatabase  # Your MongoDB utility

# Schedule giveaways to start or end
async def schedule_giveaways(db_client, producer, scheduler):
    """
    Check the database for giveaways to start or end and schedule them.
    """
    now = datetime.utcnow()
    try:
        # Fetch giveaways to start
        giveaways_to_start = await db_client.giveaways.find({
            "start_time": {"$lte": now},
            "status": "scheduled"
        }).to_list(length=None)

        # Fetch giveaways to end
        giveaways_to_end = await db_client.giveaways.find({
            "end_time": {"$lte": now},
            "status": "ongoing"
        }).to_list(length=None)

        # Fetch giveaways to update
        giveaways_to_update = await db_client.giveaways.find({
            "status": "ongoing",
            "updated": "yes",
        }).to_list(length=None)

        # Schedule start events
        for giveaway in giveaways_to_start:
            logger.info(f"Scheduling giveaway start: {giveaway['_id']}")
            scheduler.add_job(
                produce_giveaway_event,
                "date",
                run_date=giveaway["start_time"],
                args=[producer, "giveaway_start", giveaway],  # Call Kafka producer
                id=f"start-{giveaway['_id']}",
                misfire_grace_time=300,  # 5 minuteS grace period
            )
            # Update database status
            await db_client.giveaways.update_one(
                {"_id": giveaway["_id"]},
                {"$set": {"status": "ongoing"}}
            )

        # Schedule update events
        for giveaway in giveaways_to_update:
            logger.info(f"Scheduling giveaway update: {giveaway['_id']}")
            scheduler.add_job(
                produce_giveaway_event,
                "date",
                run_date=datetime.utcnow(),
                args=[producer, "giveaway_update", giveaway],  # Call Kafka producer
                id=f"update-{giveaway['_id']}",
                misfire_grace_time=300,  # 5 minutes grace period
            )
            # Update database status
            await db_client.giveaways.update_one(
                {"_id": giveaway["_id"]},
                {"$set": {"updated": "no"}}
            )

        # Schedule end events
        for giveaway in giveaways_to_end:
            logger.info(f"Scheduling giveaway end: {giveaway['_id']}")
            scheduler.add_job(
                produce_giveaway_event,
                "date",
                run_date=giveaway["end_time"],
                args=[producer, "giveaway_end", giveaway],  # Call Kafka producer
                id=f"end-{giveaway['_id']}",
                misfire_grace_time=300,  # 5 minutes grace period
            )
            # Update database status
            await db_client.giveaways.update_one(
                {"_id": giveaway["_id"]},
                {"$set": {"status": "ended"}}
            )
    except Exception as e:
        logger.error(f"Error while scheduling giveaways: {e}")


# Main function
async def main():
    """
    Main entry point for giveaway tracking.
    """
    logger.info("Starting giveaway tracker...")
    try:
        # Initialize database and Kafka producer
        db_client = MongoDatabase(
            stats_db_connection=Config.stats_mongodb,
            static_db_connection=Config.static_mongodb,
        )
        producer = AIOKafkaProducer(
            bootstrap_servers=['85.10.200.219:9092']
        )
        await producer.start()

        # Initialize the scheduler
        scheduler = AsyncIOScheduler()
        scheduler.start()

        # Run the scheduling loop
        while True:
            await schedule_giveaways(db_client, producer, scheduler)
            await asyncio.sleep(60)  # Check every minute
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {e}")
    finally:
        logger.info("Shutting down Kafka producer...")
        await producer.stop()
