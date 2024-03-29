import aiohttp
import asyncio
import coc
import pendulum as pend

from hashids import Hashids
from datetime import datetime
from msgspec.json import decode
from msgspec import Struct
from pymongo import  InsertOne, UpdateOne
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from typing import List
from utility.classes import MongoDatabase
from .config import GlobalWarTrackingConfig
from utility.keycreation import create_keys
from loguru import logger
from asyncio_throttle import Throttler

config = GlobalWarTrackingConfig()
db_client = MongoDatabase(stats_db_connection=config.stats_mongodb, static_db_connection=config.static_mongodb)
coc_client = coc.Client(key_count=10, throttle_limit=30, cache_max_size=0, raw_attribute=True)


class Members(Struct):
    tag: str

class Clan(Struct):
    tag: str
    members: List[Members]


class War(Struct):
    state: str
    preparationStartTime: str
    endTime: str
    clan: Clan
    opponent: Clan

in_war = set()

async def broadcast(scheduler: AsyncIOScheduler):
    global in_war
    x = 1
    keys = await create_keys([config.coc_email.format(x=x) for x in range(config.min_coc_email, config.max_coc_email + 1)], [config.coc_password] * config.max_coc_email)

    throttler = Throttler(rate_limit=1200, period=1)

    while True:
        async def fetch(url, session: aiohttp.ClientSession, headers, tag, throttler: Throttler):
            async with throttler:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        return ((await response.read()), tag)
                    elif response.status == 503:
                        return (503, 503)
                    return (None, None)


        pipeline = [{"$match": {"openWarLog": True}}, {"$group": {"_id": "$tag"}}]
        all_tags = [x["_id"] for x in (await db_client.global_clans.aggregate(pipeline).to_list(length=None))]
        size_break = 50000
        bot_clan_tags = await db_client.clans_db.distinct("tag")
        all_tags = [tag for tag in all_tags if tag not in in_war] + bot_clan_tags
        all_tags = list(set(all_tags))

        if x % 20 != 0:
            right_now = datetime.now().timestamp()
            one_week_ago = int(right_now) - 604800

            try:
                clan_side_tags = await db_client.clan_wars.distinct("data.clan.tag", filter={"endTime": {"$gte" : one_week_ago}})
            except Exception:
                pipeline = [{"$match": {"endTime": {"$gte": one_week_ago}}}, {"$group": {"_id": "$data.clan.tag"}}]
                clan_side_tags = [x["_id"] for x in (await db_client.clan_wars.aggregate(pipeline).to_list(length=None))]

            try:
                opponent_side_tags = await db_client.clan_wars.distinct("data.opponent.tag", filter={"endTime": {"$gte" : one_week_ago}})
            except Exception:
                pipeline = [{"$match": {"endTime": {"$gte": one_week_ago}}}, {"$group": {"_id": "$data.opponent.tag"}}]
                opponent_side_tags = [x["_id"] for x in (await db_client.clan_wars.aggregate(pipeline).to_list(length=None))]
            combined_tags = set(opponent_side_tags + clan_side_tags)
            all_tags = [tag for tag in all_tags if tag in combined_tags]

        logger.info(f"{len(all_tags)} tags")
        all_tags = [all_tags[i:i + size_break] for i in range(0, len(all_tags), size_break)]
        ones_that_tried_again = []

        x += 1
        for count, tag_group in enumerate(all_tags, 1):
            logger.info(f"Group {count}/{len(all_tags)}")
            tasks = []
            connector = aiohttp.TCPConnector(limit=500, ttl_dns_cache=600)
            async with aiohttp.ClientSession(connector=connector) as session:
                for tag in tag_group:
                    keys.rotate(1)
                    tasks.append(fetch(f"https://api.clashofclans.com/v1/clans/{tag.replace('#', '%23')}/currentwar", session, {"Authorization": f"Bearer {keys[0]}"}, tag, throttler=throttler))
                responses = await asyncio.gather(*tasks, return_exceptions=True)
                await session.close()

            responses = [r for r in responses if type(r) is tuple]
            changes = []
            war_timers = []
            for response, tag in responses:
                # we shouldnt have completely invalid tags, they all existed at some point
                if response is None or response == 503:
                    continue
                try:
                    war = decode(response, type=War)
                except:
                    continue

                if war.state != "notInWar":
                    war_end = coc.Timestamp(data=war.endTime)
                    run_time = war_end.time.replace(tzinfo=pend.UTC)
                    if war_end.seconds_until < 0:
                        continue
                    war_prep = coc.Timestamp(data=war.preparationStartTime)
                    opponent_tag = war.opponent.tag if war.opponent.tag != tag else war.clan.tag
                    in_war.add(tag)
                    in_war.add(opponent_tag)
                    war_unique_id = "-".join(sorted([war.clan.tag, war.opponent.tag])) + f"-{int(war_prep.time.timestamp())}"
                    for member in war.clan.members + war.opponent.members:
                        war_timers.append(UpdateOne({"_id" : member.tag}, {"$set" : {"clans" : [war.clan.tag, war.opponent.tag], "time" : war_end.time}}, upsert=True))
                    changes.append(InsertOne({"war_id" : war_unique_id,
                                              "clans" : [tag, opponent_tag],
                                              "endTime" : int(war_end.time.replace(tzinfo=pend.UTC).timestamp())
                                              }))
                    #schedule getting war
                    try:
                        scheduler.add_job(store_war, 'date', run_date=run_time, args=[tag, opponent_tag, int(coc.Timestamp(data=war.preparationStartTime).time.timestamp())],
                                          id=f"war_end_{tag}_{opponent_tag}", name=f"{tag}_war_end_{opponent_tag}", misfire_grace_time=1200, max_instances=250)
                    except Exception:
                        ones_that_tried_again.append(tag)
                        pass
            if changes:
                try:
                    await db_client.clan_wars.bulk_write(changes, ordered=False)
                except Exception:
                    pass

            if war_timers:
                try:
                    await db_client.war_timer.bulk_write(war_timers, ordered=False)
                except Exception:
                    pass

        if ones_that_tried_again:
            logger.info(f"{len(ones_that_tried_again)} tried again, examples: {ones_that_tried_again[:5]}")


async def store_war(clan_tag: str, opponent_tag: str, prep_time: int):
    global in_war
    hashids = Hashids(min_length=7)

    if clan_tag in in_war:
        in_war.remove(clan_tag)
    if opponent_tag in in_war:
        in_war.remove(opponent_tag)

    async def get_war(clan_tag : str):
        try:
            war = await coc_client.get_clan_war(clan_tag=clan_tag)
            return war
        except (coc.NotFound, coc.errors.Forbidden, coc.errors.PrivateWarLog):
            return "no access"
        except coc.errors.Maintenance:
            return "maintenance"
        except Exception:
            return "error"

    switched = False
    war = None
    war_found = False
    while not war_found:
        war = await get_war(clan_tag=clan_tag)
        if isinstance(war, coc.ClanWar):
            if war.preparation_start_time is None or int(war.preparation_start_time.time.timestamp()) != prep_time:
                if not switched:
                    clan_tag = opponent_tag
                    switched = True
                    continue
                else:
                    break
            elif war.state == "warEnded":
                war_found = True
                break
        elif war == "maintenance":
            await asyncio.sleep(30)
            continue
        elif war == "no access":
            if not switched:
                clan_tag = opponent_tag
                switched = True
                continue
            else:
                break
        elif war == "error":
            break
        await asyncio.sleep(war._response_retry)

    if not war_found:
        return

    war_unique_id = "-".join(sorted([war.clan.tag, war.opponent.tag])) + f"-{int(war.preparation_start_time.time.timestamp())}"

    war_result = await db_client.clan_wars.find_one({"war_id" : war_unique_id})
    if war_result.get("data") is not None:
        return

    custom_id = hashids.encode(int(war.preparation_start_time.time.timestamp()) + int(pend.now(tz=pend.UTC).timestamp()))
    await db_client.clan_wars.update_one({"war_id": war_unique_id},
        {"$set" : {
        "custom_id": custom_id,
        "data": war._raw_data,
        "type" : war.type}}, upsert=True)


async def main():
    scheduler = AsyncIOScheduler(timezone=pend.UTC)
    scheduler.start()
    await broadcast(scheduler=scheduler)



