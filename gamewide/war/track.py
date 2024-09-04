import asyncio
import logging
import random
from datetime import datetime
from typing import List

import aiohttp
import coc
import orjson
import pendulum as pend
from asyncio_throttle import Throttler
from expiring_dict import ExpiringDict
from hashids import Hashids
from kafka import KafkaProducer
from loguru import logger
from msgspec import Struct
from msgspec.json import decode
from pymongo import InsertOne, UpdateOne

from utility.classes import MongoDatabase
from utility.keycreation import create_keys

from .config import GlobalWarTrackingConfig

config = GlobalWarTrackingConfig()
db_client = MongoDatabase(
    stats_db_connection=config.stats_mongodb,
    static_db_connection=config.static_mongodb,
)
coc_client = coc.Client(
    key_count=10,
    throttle_limit=200,
    cache_max_size=0,
    raw_attribute=True,
    timeout=600,
)


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


async def broadcast():
    in_war = ExpiringDict()

    x = 1
    keys = await create_keys(
        [
            config.coc_email.format(x=x)
            for x in range(config.min_coc_email, config.max_coc_email + 1)
        ],
        [config.coc_password] * config.max_coc_email,
    )
    producer = KafkaProducer(
        bootstrap_servers=['85.10.200.219:9092'], api_version=(3, 6, 0)
    )

    throttler = Throttler(rate_limit=1200, period=1)
    print(f'{len(list(keys))} keys')
    await coc_client.login_with_tokens(*list(keys))

    while True:
        api_fails = 0

        async def fetch(
            url,
            session: aiohttp.ClientSession,
            headers,
            tag,
            throttler: Throttler,
        ):
            async with throttler:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        return ((await response.read()), tag)
                    elif response.status == 403:
                        return (403, 403)
                    return (None, None)

        bot_clan_tags = await db_client.clans_db.distinct('tag')
        size_break = 50_000

        if x % 20 != 0:
            right_now = pend.now(tz=pend.UTC).timestamp()
            one_week_ago = int(right_now) - (604800 * 2)

            pipeline = [
                {
                    '$match': {
                        '$and': [
                            {'endTime': {'$gte': one_week_ago}},
                            {'type': {'$ne': 'cwl'}},
                        ]
                    }
                },
                {'$group': {'_id': '$clans'}},
            ]
            results = await db_client.clan_wars.aggregate(pipeline).to_list(
                length=None
            )
            clan_tags = []
            for result in results:
                clan_tags.extend(result.get('_id', []))

            combined_tags = set(clan_tags + bot_clan_tags)
            all_tags = list(
                [tag for tag in combined_tags if tag not in in_war]
            )
        else:
            pipeline = [
                {'$match': {'openWarLog': True}},
                {'$group': {'_id': '$tag'}},
            ]
            all_tags = [
                x['_id']
                for x in (
                    await db_client.global_clans.aggregate(pipeline).to_list(
                        length=None
                    )
                )
            ]
            all_tags = [
                tag for tag in all_tags if tag not in in_war
            ] + bot_clan_tags
            all_tags = list(set(all_tags))

        logger.info(f'{len(all_tags)} tags')
        all_tags = [
            all_tags[i : i + size_break]
            for i in range(0, len(all_tags), size_break)
        ]

        timers_alr_captured = set()
        if x == 1:
            right_now = datetime.now().timestamp()
            one_week_ago = int(right_now)
            pipeline = [
                {
                    '$match': {
                        '$and': [
                            {'endTime': {'$gte': one_week_ago}},
                            {'data': {'$eq': None}},
                        ]
                    }
                },
                {'$group': {'_id': '$war_id'}},
            ]
            results = await db_client.clan_wars.aggregate(pipeline).to_list(
                length=None
            )
            for result in results:
                timers_alr_captured.add(result.get('_id'))

        x += 1
        for count, tag_group in enumerate(all_tags, 1):
            logger.info(f'Group {count}/{len(all_tags)}')
            tasks = []
            connector = aiohttp.TCPConnector(limit=500, ttl_dns_cache=600)
            async with aiohttp.ClientSession(connector=connector) as session:
                for tag in tag_group:
                    if tag not in in_war:
                        keys.rotate(1)
                        tasks.append(
                            fetch(
                                f"https://api.clashofclans.com/v1/clans/{tag.replace('#', '%23')}/currentwar",
                                session,
                                {'Authorization': f'Bearer {keys[0]}'},
                                tag,
                                throttler=throttler,
                            )
                        )
                responses = await asyncio.gather(
                    *tasks, return_exceptions=True
                )
                await session.close()

            responses = [r for r in responses if type(r) is tuple]
            changes = []
            war_timers = []
            for response, tag in responses:
                # we shouldnt have completely invalid tags, they all existed at some point
                if response is None or response == 403:
                    if response is None:
                        api_fails += 1
                    continue
                try:
                    war = decode(response, type=War)
                except:
                    continue

                if war.state != 'notInWar':
                    war_end = coc.Timestamp(data=war.endTime)
                    run_time = war_end.time.replace(tzinfo=pend.UTC)
                    if war_end.seconds_until < 0:
                        continue
                    war_prep = coc.Timestamp(data=war.preparationStartTime)
                    war_prep = war_prep.time.replace(tzinfo=pend.UTC)

                    opponent_tag = (
                        war.opponent.tag
                        if war.opponent.tag != tag
                        else war.clan.tag
                    )

                    in_war.ttl(key=tag, value=True, ttl=war_end.seconds_until)
                    in_war.ttl(
                        key=opponent_tag, value=True, ttl=war_end.seconds_until
                    )

                    war_unique_id = (
                        '-'.join(sorted([war.clan.tag, war.opponent.tag]))
                        + f'-{int(war_prep.timestamp())}'
                    )
                    if war_unique_id not in timers_alr_captured:
                        for member in war.clan.members + war.opponent.members:
                            war_timers.append(
                                UpdateOne(
                                    {'_id': member.tag},
                                    {
                                        '$set': {
                                            'clans': [
                                                war.clan.tag,
                                                war.opponent.tag,
                                            ],
                                            'time': war_end.time,
                                        }
                                    },
                                    upsert=True,
                                )
                            )

                        changes.append(
                            InsertOne(
                                {
                                    'war_id': war_unique_id,
                                    'clans': [tag, opponent_tag],
                                    'endTime': int(run_time.timestamp()),
                                }
                            )
                        )
                    json_data = {
                        'tag': tag,
                        'opponent_tag': opponent_tag,
                        'prep_time': int(war_prep.timestamp()),
                        'run_time': int(run_time.timestamp()),
                    }
                    producer.send(
                        topic='war_store', value=orjson.dumps(json_data)
                    )

            if changes:
                try:
                    await db_client.clan_wars.bulk_write(
                        changes, ordered=False
                    )
                except Exception:
                    pass

            if war_timers:
                try:
                    await db_client.war_timer.bulk_write(
                        war_timers, ordered=False
                    )
                except Exception:
                    pass

            await asyncio.sleep(5)

        if api_fails != 0:
            logger.info(f'{api_fails} API call fails')

        logger.info(f'{len(in_war)} clans in war')


async def main():
    await broadcast()
