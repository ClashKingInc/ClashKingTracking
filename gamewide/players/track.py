import asyncio
from collections import deque

import aiohttp
import pytz
import ujson
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from pymongo import InsertOne

from utility.classes import MongoDatabase
from utility.keycreation import create_keys

from .config import GlobalPlayerTrackingConfig

utc = pytz.utc


async def fetch(url, session: aiohttp.ClientSession, headers):
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            return await response.read()
        return None


async def broadcast():
    config = GlobalPlayerTrackingConfig()

    db_client = MongoDatabase(
        stats_db_connection=config.stats_mongodb,
        static_db_connection=config.static_mongodb,
    )

    keys: deque = await create_keys(
        [
            config.coc_email.format(x=x)
            for x in range(config.min_coc_email, config.max_coc_email + 1)
        ],
        [config.coc_password] * config.max_coc_email,
    )
    tag_pipeline = [
        {'$unwind': '$memberList'},
        {'$match': {'memberList.townhall': {'$gte': 9}}},
        {'$group': {'_id': '$memberList.tag'}},
    ]
    tags = await db_client.global_clans.aggregate(tag_pipeline).to_list(
        length=None
    )
    all_tags = [x['_id'] for x in tags]

    print(f'got {len(all_tags)} tags')

    keys = deque(keys)
    size_break = 100000
    all_tags = [
        all_tags[i : i + size_break]
        for i in range(0, len(all_tags), size_break)
    ]

    for tag_group in all_tags:
        tasks = []
        connector = TCPConnector(limit=1000, enable_cleanup_closed=True)
        timeout = ClientTimeout(total=3600)
        async with ClientSession(
            connector=connector, timeout=timeout
        ) as session:
            for tag in tag_group:
                keys.rotate(1)
                tasks.append(
                    fetch(
                        f"https://api.clashofclans.com/v1/players/{tag.replace('#', '%23')}",
                        session,
                        {'Authorization': f'Bearer {keys[0]}'},
                    )
                )
            responses = await asyncio.gather(*tasks)
            await session.close()

        print(f'fetched {len(responses)} responses')
        changes = []
        for response in responses:   # type: bytes
            # we shouldnt have completely invalid tags, they all existed at some point
            if response is None:
                continue

            response: dict = ujson.loads(response)
            response['_id'] = response.pop('tag')
            try:
                del response['legendStatistics']
            except Exception:
                pass
            changes.append(InsertOne(response))

        if changes:
            results = await db_client.global_players.bulk_write(
                changes, ordered=False
            )
            print(results.bulk_api_result)
