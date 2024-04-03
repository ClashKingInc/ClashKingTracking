import asyncio
import random

from utility.classes import MongoDatabase
from meilisearch_python_sdk import AsyncClient
from utility.config import Config
from loguru import logger
import orjson
import snappy
import aiohttp
from redis import asyncio as redis
import pendulum as pend
import time

async def main():
    config = Config()
    db_client = MongoDatabase(stats_db_connection=config.stats_mongodb, static_db_connection=config.static_mongodb)

    cache = redis.Redis(host=config.redis_ip, port=6379, db=0, password=config.redis_pw, decode_responses=False, max_connections=50,
                        health_check_interval=10, socket_connect_timeout=5, retry_on_timeout=True, socket_keepalive=True)

    # change to find changes in last 20 mins
    current_time = pend.now(tz=pend.UTC)
    time_20_mins_ago = current_time.subtract(minutes=20).timestamp()
    # any players that had an update in last 20 mins
    pipeline = [{"$match": {}},
                {"$project": {"tag": "$tag"}},
                {"$unset": "_id"}]
    all_player_tags = [x["tag"] for x in (await db_client.player_stats.aggregate(pipeline).to_list(length=None))]
    # delete any tags that are gone
    # await db_client.player_autocomplete.delete_many({"tag" : {"$nin" : all_player_tags}})
    split_size = 50_000
    split_tags = [all_player_tags[i:i + split_size] for i in range(0, len(all_player_tags), split_size)]

    for count, group in enumerate(split_tags, 1):
        t = time.time()
        print(f"Group {count}/{len(split_tags)}")
        docs_to_insert = []
        previous_player_responses = await cache.mget(keys=group)
        for response in previous_player_responses:
            if response is not None:
                response = orjson.loads(snappy.decompress(response))
                d = {
                    "name": response.get("name"),
                    "clan": response.get("clan", {}).get("tag", "No Clan"),
                    "league": response.get("league", {}).get("name", "Unranked"),
                    "id": response.get("tag").replace("#", ''),
                    "th": response.get("townHallLevel"),
                    "clan_name": response.get("clan", {}).get("name", "No Clan"),
                    "trophies": response.get("trophies")
                }
                docs_to_insert.append(d)
        print(f"starting bulk write: took {time.time() - t} secs")


        async def add_documents(documents):
            headers = {"Authorization" : f"Bearer {config.meili_pw}"}
            async with aiohttp.ClientSession() as session:
                await asyncio.sleep(random.randint(0, 50)/10)
                await session.post('http://85.10.200.219:7700/indexes/players/documents', headers=headers, json=documents)

        await add_documents(documents=docs_to_insert)
        await asyncio.sleep(15)
