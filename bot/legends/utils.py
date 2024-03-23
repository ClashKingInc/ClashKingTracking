import pendulum as pend
import aiohttp
import asyncio
import snappy
import orjson
import time

from kafka import KafkaProducer
from msgspec import Struct
from msgspec.json import decode
from typing import Union, List, Optional
from pymongo import InsertOne, UpdateOne
from collections import deque, defaultdict
from utility.classes import MongoDatabase
from utility.utils import gen_season_date, gen_raid_date, gen_games_season, gen_legend_date


async def get_player_responses(keys: deque, tags: list[str]):
    tasks = []
    connector = aiohttp.TCPConnector(limit=2000, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=1800)
    session = aiohttp.ClientSession(connector=connector, timeout=timeout)
    for tag in tags:
        keys.rotate(1)

        async def fetch(url, session: aiohttp.ClientSession, headers):
            new_response = await session.get(url, headers=headers)
            t = f'#{url.split("%23")[-1]}'
            if new_response.status == 404:  # remove banned players
                return (t, "delete")
            elif new_response.status != 200:
                return (t, None)
            new_response = await new_response.read()
            return (t, new_response)

        tasks.append(fetch(url=f'https://api.clashofclans.com/v1/players/{tag.replace("#", "%23")}', session=session, headers={"Authorization": f"Bearer {keys[0]}"}))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    await session.close()
    return results