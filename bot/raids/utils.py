import asyncio
from typing import Dict, List
import coc
import pendulum as pend
import ujson
from kafka import KafkaProducer
from pymongo import UpdateOne

from utility.classes import MongoDatabase

CLAN_CACHE = {}

async def raid_weekend_track(
    clan_tags: List[str],
    db_client: MongoDatabase,
    coc_client: coc.Client,
    producer: KafkaProducer,
):
    cached_raids = await db_client.capital_cache.find(
        {'tag': {'$in': clan_tags}}
    ).to_list(length=None)
    cached_raids = {r.get('tag'): r.get('data') for r in cached_raids}

    tasks = []
    for clan_tag in clan_tags:
        tasks.append(coc_client.get_raid_log(clan_tag=clan_tag, limit=1))
    current_raids = await asyncio.gather(*tasks, return_exceptions=True)

    current_raids: Dict[str, coc.RaidLogEntry] = {
        r[0].clan_tag: r[0]
        for r in current_raids
        if r and not isinstance(r, BaseException)
    }

    db_updates = []
    for clan_tag in clan_tags:
        current_raid = current_raids.get(clan_tag)
        previous_raid = cached_raids.get(clan_tag)

        if current_raid is None:
            continue

        if current_raid._raw_data != previous_raid:
            db_updates.append(
                UpdateOne(
                    {'tag': clan_tag},
                    {'$set': {'data': current_raid._raw_data}},
                    upsert=True,
                )
            )

            if previous_raid is None:
                continue

            previous_raid = coc.RaidLogEntry(
                data=previous_raid, client=coc_client, clan_tag=clan_tag
            )

            if previous_raid.attack_log:
                new_clans = (
                    clan
                    for clan in current_raid.attack_log
                    if clan not in previous_raid.attack_log
                )
            else:
                new_clans = current_raid.attack_log

            for clan in new_clans:
                json_data = {
                    'type': 'new_offensive_opponent',
                    'clan': clan._raw_data,
                    'clan_tag': clan_tag,
                    'raid': current_raid._raw_data,
                }
                producer.send(
                    'capital',
                    ujson.dumps(json_data).encode('utf-8'),
                    key=clan_tag.encode('utf-8'),
                    timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000),
                )

            attacked = []
            for member in current_raid.members:
                old_member = coc.utils.get(
                    previous_raid.members, tag=member.tag
                )
                if old_member is None or (
                    old_member.attack_count != member.attack_count
                ):
                    attacked.append(member.tag)

            clan_data = CLAN_CACHE.get(clan_tag)

            if clan_data:
                json_data = {
                    'type': 'raid_attacks',
                    'clan_tag': current_raid.clan_tag,
                    'attacked': attacked,
                    'raid': current_raid._raw_data,
                    'old_raid': previous_raid._raw_data,
                    'clan': clan_data._raw_data,
                }
                producer.send(
                    'capital',
                    ujson.dumps(json_data).encode('utf-8'),
                    key=clan_tag.encode('utf-8'),
                    timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000),
                )

            if current_raid.state != previous_raid.state:
                json_data = {
                    'type': 'raid_state',
                    'clan_tag': current_raid.clan_tag,
                    'old_raid': previous_raid._raw_data,
                    'raid': current_raid._raw_data,
                }
                producer.send(
                    'capital',
                    ujson.dumps(json_data).encode('utf-8'),
                    key=clan_tag.encode('utf-8'),
                    timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000),
                )

    if db_updates:
        await db_client.capital_cache.bulk_write(db_updates, ordered=False)
