import asyncio
import coc
import pendulum as pend
import random
import logging
import ujson

from hashids import Hashids
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from utility.classes import MongoDatabase
from .config import GlobalWarTrackingConfig
from utility.keycreation import create_keys
from loguru import logger
from aiokafka import AIOKafkaConsumer

config = GlobalWarTrackingConfig()

coc_client = coc.Client(key_count=10, throttle_limit=200, cache_max_size=0, raw_attribute=True, timeout=600)
db_client = MongoDatabase(stats_db_connection=config.stats_mongodb, static_db_connection=config.static_mongodb)


async def store(scheduler: AsyncIOScheduler):

    keys = await create_keys([config.coc_email.format(x=x) for x in range(39, 40 + 1)], [config.coc_password] * 40, as_list=True)
    await coc_client.login_with_tokens(*keys)

    topics = ["war_store"]
    consumer: AIOKafkaConsumer = AIOKafkaConsumer(*topics, bootstrap_servers='85.10.200.219:9092', auto_offset_reset="latest")
    await consumer.start()
    logger.info("Events Started")
    async for msg in consumer:
        msg = ujson.loads(msg.value)
        run_time = pend.from_timestamp(timestamp=msg.get("run_time"), tz=pend.UTC)
        try:
            scheduler.add_job(store_war, 'date', run_date=run_time, args=[msg.get("tag"), msg.get("opponent_tag"), msg.get("prep_time")], misfire_grace_time=None, max_instances=1)
        except Exception:
            pass


async def store_war(clan_tag: str, opponent_tag: str, prep_time: int):
    hashids = Hashids(min_length=7)

    async def find_active_war(clan_tag: str, opponent_tag: str, prep_time: int):
        async def get_war(clan_tag: str):
            try:
                war = await coc_client.get_clan_war(clan_tag=clan_tag)
                return war
            except (coc.NotFound, coc.errors.Forbidden, coc.errors.PrivateWarLog):
                return "no access"
            except coc.errors.Maintenance:
                return "maintenance"
            except Exception as e:
                logger.error(str(e))
                return "error"

        switched = False
        tries = 0
        while True:
            war = await get_war(clan_tag=clan_tag)

            if isinstance(war, coc.ClanWar):
                if war.state == "warEnded":
                    return war  # Found the completed war
                # Check prep time and retry if needed
                if war.preparation_start_time is None or int(war.preparation_start_time.time.replace(tzinfo=pend.UTC).timestamp()) != prep_time:
                    if not switched:
                        clan_tag = opponent_tag
                        switched = True
                        continue  # Try with the opponent's tag
                    else:
                        return None  # Both tags checked, no valid war found
            elif war == "maintenance":
                await asyncio.sleep(15 * 60)  # Wait 15 minutes for maintenance, then continue loop
                continue
            elif war == "error":
                break  # Stop on error
            elif war == "no access":
                if not switched:
                    clan_tag = opponent_tag
                    switched = True
                    continue  # Access issue, switch clan tag
                else:
                    return None  # Both tags checked, no access to either

            await asyncio.sleep(min(war._response_retry, 120))  # Wait before retry based on response retry attribute
            tries += 1
            if tries == 10:
                break

        return None

    war = await find_active_war(clan_tag=clan_tag, opponent_tag=opponent_tag, prep_time=prep_time)

    if war is None:
        return

    war_unique_id = "-".join(sorted([war.clan.tag, war.opponent.tag])) + f"-{int(war.preparation_start_time.time.replace(tzinfo=pend.UTC).timestamp())}"

    custom_id = hashids.encode(int(war.preparation_start_time.time.replace(tzinfo=pend.UTC).timestamp()) + int(pend.now(tz=pend.UTC).timestamp()) + random.randint(1000000000, 9999999999))
    await db_client.clan_wars.update_one({"war_id": war_unique_id},
                                         {"$set": {
                                             "custom_id": custom_id,
                                             "data": war._raw_data,
                                             "type": war.type}}, upsert=True)


async def main():
    scheduler = AsyncIOScheduler(timezone='UTC')
    scheduler.start()
    await store(scheduler=scheduler)



