import time

from redis import asyncio as redis

import asyncio
import collections
import pytz
from loguru import logger

from pymongo import UpdateOne, InsertOne
from datetime import timedelta


import ujson
import coc
import aiohttp

from kafka import KafkaProducer
from utility.http import HTTPClient, Route
from collections import deque
from datetime import datetime
from utility.utils import is_raids
from typing import List
from utility.keycreation import create_keys
from .utils import get_war_responses, handle_war_responses
from .config import BotClanTrackingConfig
from utility.classes import MongoDatabase


CAPITAL_CACHE = {}
CAPITAL_ATTACK_CACHE = {}
CLAN_CACHE = {}
REMINDERS = []


async def main():
    config = BotClanTrackingConfig()
    http_client = HTTPClient()

    producer = KafkaProducer(bootstrap_servers=["85.10.200.219:9092"], api_version=(3, 6, 0))
    db_client = MongoDatabase(stats_db_connection=config.stats_mongodb, static_db_connection=config.static_mongodb)

    keys: deque = await create_keys([config.coc_email.format(x=x) for x in range(config.min_coc_email, config.max_coc_email + 1)], [config.coc_password] * config.max_coc_email)
    coc_client = coc.Client(raw_attribute=True)


    while True:
        r = time.time()
        clan_tags = await db_client.clans_db.distinct("tag")

        war_responses = await get_war_responses(http_client=http_client, keys=keys, db_client=db_client, clan_tags=clan_tags)

        await handle_war_responses(coc_client=coc_client, producer=producer, responses=war_responses)


        #if war state is "warEnded", send war_end event

        if is_raids():
            capital_tasks = []
            for tag in clan_tags:
                headers = {"Authorization": f"Bearer {keys[0]}"}
                keys.rotate(1)

                async def get_raid(clan_tag: str, headers):
                    clan_tag_for_url = clan_tag.replace('#', '%23')
                    raid_log = await http_client.request(Route("GET", f"/clans/{clan_tag_for_url}/capitalraidseasons?limit=1"), headers=headers)
                    if len(raid_log.get("items", [])) == 0:
                        return None
                    raid = coc.RaidLogEntry(data=raid_log.get("items")[0], client=coc_client, clan_tag=clan_tag)
                    previous_raid = CAPITAL_CACHE.get(clan_tag)
                    CAPITAL_CACHE[clan_tag] = raid

                    if previous_raid is None:
                        return None

                    if previous_raid.attack_log:
                        new_clans = (clan for clan in raid.attack_log if clan not in previous_raid.attack_log)
                    else:
                        new_clans = raid.attack_log

                    for clan in new_clans:
                        json_data = {"type": "new_offensive_opponent", "clan": clan._raw_data, "clan_tag": clan_tag, "raid" : raid._raw_data}
                        producer.send("capital", ujson.dumps(json_data).encode("utf-8"), timestamp_ms=int(datetime.now(tz=pytz.utc).timestamp() * 1000))

                    changes = []
                    for member in raid.members:
                        CAPITAL_ATTACK_CACHE[member.tag] = (member.attack_count, member.attack_limit + member.bonus_attack_limit)
                        old_member = coc.utils.get(previous_raid.members, tag=member.tag)
                        if old_member is None:
                            continue
                        if old_member.attack_count != member.attack_count:
                            changes.append((old_member, member))

                    for old_member, member in changes:
                        json_data = {"type": "raid_attacks",
                                     "clan_tag": raid.clan_tag,
                                     "old_member": old_member._raw_data,
                                     "new_member": member._raw_data,
                                     "raid": raid._raw_data}
                        producer.send("capital", ujson.dumps(json_data).encode("utf-8"), timestamp_ms=int(datetime.now(tz=pytz.utc).timestamp() * 1000))

                    if raid.state != previous_raid.state:
                        json_data = {"type": "raid_state", "clan_tag": raid.clan_tag, "old_raid": previous_raid._raw_data, "raid": raid._raw_data}
                        producer.send("capital", ujson.dumps(json_data).encode("utf-8"), timestamp_ms=int(datetime.now(tz=pytz.utc).timestamp() * 1000))

                capital_tasks.append(get_raid(clan_tag=tag, headers=headers))

            await asyncio.gather(*capital_tasks)


        clan_tasks = []
        connector = aiohttp.TCPConnector(limit=500)
        async with aiohttp.ClientSession(connector=connector) as session:
            for tag in clan_tags:
                headers = {"Authorization": f"Bearer {keys[0]}"}
                tag = tag.replace("#", "%23")
                url = f"https://api.clashofclans.com/v1/clans/{tag}"
                keys.rotate(1)

                async def fetch(url, session, headers):
                    async with session.get(url, headers=headers) as response:
                        if response.status == 200:
                            return (await response.json())
                        return None

                clan_tasks.append(fetch(url, session, headers))
            clan_responses = await asyncio.gather(*clan_tasks, return_exceptions=True)
            await session.close()

        print(f"{len(clan_responses)} clans")
        for count, clan_response in enumerate(clan_responses):
            if clan_response is None:
                continue
            clan = coc.Clan(data=clan_response, client=coc_client)

            previous_clan: coc.Clan = CLAN_CACHE.get(clan.tag)
            CLAN_CACHE[clan.tag] = clan

            if previous_clan is None:
                continue

            attributes = ["level", "type", "description", "location", "capital_league", "required_townhall", "required_trophies", "war_win_streak", "war_league", "member_count"]
            for attribute in attributes:
                if getattr(clan, attribute) != getattr(previous_clan, attribute):
                    json_data = {"type": attribute, "old_clan": previous_clan._raw_data, "new_clan": clan._raw_data}
                    producer.send("clan", ujson.dumps(json_data).encode("utf-8"), timestamp_ms=int(datetime.now(tz=pytz.utc).timestamp() * 1000))
            current_tags = set(n.tag for n in previous_clan.members)
            if current_tags:
                # we can't check the member_count first incase 1 person left and joined within the 60sec.
                members_joined = (n for n in clan.members if n.tag not in current_tags)
                for member in members_joined:
                    json_data = {"type": "member_join", "clan": clan._raw_data, "member" : member._raw_data}
                    producer.send("clan", ujson.dumps(json_data).encode("utf-8"), timestamp_ms=int(datetime.now(tz=pytz.utc).timestamp() * 1000))

            current_tags = set(n.tag for n in clan.members)
            if current_tags:
                members_left = (n for n in previous_clan.members if n.tag not in current_tags)
                for member in members_left:
                    json_data = {"type": "member_leave", "clan": clan._raw_data, "member": member._raw_data}
                    producer.send("clan", ujson.dumps(json_data).encode("utf-8"), timestamp_ms=int(datetime.now(tz=pytz.utc).timestamp() * 1000))

        print(f"finished: {time.time() - r}")







