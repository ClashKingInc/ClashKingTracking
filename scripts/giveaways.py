import asyncio
from datetime import datetime

import pendulum as pend
from loguru import logger

from .tracking import Tracking
from utility.config import TrackingType


class GiveawayTracking(Tracking):
    def __init__(self):
        super().__init__(tracker_type=TrackingType.GIVEAWAY)

    async def _send_giveaway_event(self, event_type, giveaway):
        """
        Send a Kafka event for the given giveaway.
        """
        # Build the base message
        message = {"type": event_type, "giveaway": giveaway}

        logger.info(f"Sending Kafka event: {message}")
        # Send the message to Kafka
        self._send_to_kafka(topic="giveaway", data=message, key=None)

    # Schedule giveaways to start or end
    async def _schedule_giveaways(self):
        """
        Check the database for giveaways to start or end and schedule them.
        """
        now = pend.now(tz=pend.UTC)
        try:
            # Fetch giveaways to start
            giveaways_to_start = self.mongo.giveaways.find(
                {"start_time": {"$lte": now}, "status": "scheduled"}
            ).to_list(length=None)

            # Fetch giveaways to end
            giveaways_to_end = self.mongo.giveaways.find({"end_time": {"$lte": now}, "status": "ongoing"}).to_list(
                length=None
            )

            # Fetch giveaways to update
            giveaways_to_update = self.mongo.giveaways.find({"status": "ongoing", "updated": "yes"}).to_list(
                length=None
            )

            # Schedule start events
            for giveaway in giveaways_to_start:
                logger.info(f"Scheduling giveaway start: {giveaway['_id']}")
                self.scheduler.add_job(
                    self._send_giveaway_event,
                    "date",
                    run_date=giveaway["start_time"],
                    args=["giveaway_start", giveaway],  # Call Kafka producer
                    id=f"start-{giveaway['_id']}",
                    misfire_grace_time=600,  # 5 minuteS grace period
                )
                # Update database status
                self.mongo.giveaways.update_one({"_id": giveaway["_id"]}, {"$set": {"status": "ongoing"}})

            # Schedule update events
            for giveaway in giveaways_to_update:
                logger.info(f"Scheduling giveaway update: {giveaway['_id']}")
                self.scheduler.add_job(
                    self._send_giveaway_event,
                    "date",
                    run_date=pend.now(tz=pend.UTC),
                    args=["giveaway_update", giveaway],
                    id=f"update-{giveaway['_id']}",
                    misfire_grace_time=600,
                )
                # Update database status
                await self.mongo.giveaways.update_one({"_id": giveaway["_id"]}, {"$set": {"updated": "no"}})

            # Schedule end events
            for giveaway in giveaways_to_end:
                logger.info(f"Scheduling giveaway end: {giveaway['_id']}")
                self.scheduler.add_job(
                    self._send_giveaway_event,
                    "date",
                    run_date=giveaway["end_time"],
                    args=["giveaway_end", giveaway],
                    id=f"end-{giveaway['_id']}",
                    misfire_grace_time=None,
                )
                # Update database status
                await self.mongo.giveaways.update_one({"_id": giveaway["_id"]}, {"$set": {"status": "ended"}})
        except Exception as e:
            logger.error(f"Error while scheduling giveaways: {e}")

    async def run(self):
        await self.initialize()
        while True:
            await self._schedule_giveaways()
            await asyncio.sleep(60)  # Check every minute


asyncio.run(GiveawayTracking().run())
