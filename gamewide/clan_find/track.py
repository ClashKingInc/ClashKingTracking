import ujson
import coc
import collections
import aiohttp
import asyncio
import pendulum as pend

from .config import GlobalClanFindConfig
from coc import Timestamp
from utility.keycreation import create_keys
from utility.classes import MongoDatabase
from datetime import datetime
from pymongo import InsertOne
from loguru import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler
scheduler = AsyncIOScheduler(timezone=pend.UTC)
scheduler.start()


@scheduler.scheduled_job("cron", day_of_week="wed", hour=10)
async def broadcast():
    config = GlobalClanFindConfig()

    db_client = MongoDatabase(stats_db_connection=config.stats_mongodb, static_db_connection=config.static_mongodb)

    keys = await create_keys([config.coc_email.format(x=x) for x in range(config.min_coc_email, config.max_coc_email + 1)], [config.coc_password] * config.max_coc_email)
    logger.info(f"{len(keys)} keys created")

    coc_client = coc.Client(key_count=10, throttle_limit=25, cache_max_size=0, raw_attribute=True)
    coc_client.login_with_keys(*keys[:10])

    async def fetch(url, session: aiohttp.ClientSession, headers):
        async with session.get(url, headers=headers) as response:
            return (await response.read())

    pipeline = [{"$match": {}}, {"$group": {"_id": "$tag"}}]
    all_tags = [x["_id"] for x in (await db_client.global_clans.aggregate(pipeline).to_list(length=None))]
    clan_tags_alr_found = set(all_tags)
    size_break = 50000
    all_tags = [all_tags[i:i + size_break] for i in range(0, len(all_tags), size_break)]

    for tag_group in all_tags:
        tags_to_add = []
        tasks = []
        deque = collections.deque
        connector = aiohttp.TCPConnector(limit=250, ttl_dns_cache=300)
        keys = deque(keys)
        url = "https://api.clashofclans.com/v1/clans/"
        async with aiohttp.ClientSession(connector=connector) as session3:
            for tag in tag_group:
                tag = tag.replace("#", "%23")
                keys.rotate(1)
                tasks.append(fetch(f"{url}{tag}/warlog?limit=200", session3, {"Authorization": f"Bearer {keys[0]}"}))
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            await session3.close()
        right_now = int(datetime.now().timestamp())
        for response in responses:
            try:
                warlog = ujson.loads(response)
            except:
                continue

            for item in warlog.get("items", []):
                if item.get("opponent").get("tag") is None:
                    continue
                del item["clan"]["badgeUrls"]
                del item["opponent"]["badgeUrls"]
                t = int(Timestamp(data=item["endTime"]).time.timestamp())
                item["timeStamp"] = t
                if (right_now - t <= 2592000) and item["opponent"]["tag"] not in clan_tags_alr_found:
                    tags_to_add.append(InsertOne({"tag" : item["opponent"]["tag"]}))

        if tags_to_add:
            try:
                results = await db_client.global_clans.bulk_write(tags_to_add, ordered=False)
                print(results.bulk_api_result)
            except Exception:
                print(f"potentially added {len(tags_to_add)} tags")

        '''if items_to_push:
            try:
                results = await war_logs.bulk_write(items_to_push, ordered=False)
                print(results.bulk_api_result)
            except Exception:
                print(f"potentially added {len(items_to_push)} logs")'''


loop = asyncio.get_event_loop()
loop.run_forever()
