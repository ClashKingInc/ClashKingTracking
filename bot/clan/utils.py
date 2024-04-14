import pendulum as pend
import asyncio
import coc
import ujson

from datetime import timedelta
from kafka import KafkaProducer
from utility.http import HTTPClient, Route
from typing import List, Dict
from loguru import logger
from collections import deque
from pymongo import UpdateOne
from utility.classes import MongoDatabase
from expiring_dict import ExpiringDict

WAR_CACHE = ExpiringDict()
CLAN_CACHE = {}

reminder_times = [f"{int(time)}hr" if time.is_integer() else f"{time}hr" for time in (x * 0.25 for x in range(1, 193))]


def send_reminder(time: str, war_unique_id: str, clan_tag: str ,producer: KafkaProducer):
    global WAR_CACHE
    war = WAR_CACHE.get(war_unique_id)
    if war is None:
        return

    json_data = {
        "type" : "war",
        "clan_tag" : clan_tag,
        "time" : time,
        "data" : war._raw_data
    }
    producer.send("reminder", ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))



async def clan_war_track(clan_tag: str, db_client: MongoDatabase, coc_client: coc.Client, producer: KafkaProducer):
    loop = asyncio.get_event_loop()

    war = None
    try:
        war = await coc_client.get_current_war(clan_tag=clan_tag)
    except coc.errors.PrivateWarLog:
        result = await db_client.clan_wars.find_one({"$and": [{"endTime": {"$gte": pend.now(tz=pend.UTC).now()}}, {"clans": clan_tag}]})
        if result is not None:
            other_clan = result.get("clans", []).remove(clan_tag)
            try:
                war = await coc_client.get_current_war(clan_tag=other_clan)
            except coc.errors.PrivateWarLog:
                pass
    except Exception:
        war = None

    next_round = None
    if war is not None and war.is_cwl and war.state != "inPreparation" and war.league_group.state != "ended":
        try:
            next_round = await coc_client.get_current_war(clan_tag=clan_tag, cwl_round=coc.WarRound.current_preparation)
        except Exception:
            pass

    war_list = [w for w in [war, next_round] if w is not None]

    for war in war_list:
        #notInWar state, skip
        if war.preparation_start_time is None:
            continue

        war_unique_id = "-".join(sorted([war.clan_tag, war.opponent.tag])) + f"-{int(war.preparation_start_time.time.timestamp())}"

        previous_war = WAR_CACHE.get(war_unique_id)
        if previous_war is None:
            WAR_CACHE[war_unique_id] = war

            async def schedule_reminders(reminder_times, war_end_time: coc.Timestamp):
                acceptable_times = []
                for r_time in reversed(reminder_times):
                    time = r_time.replace("hr", "")
                    time = int(float(time) * 3600)
                    if war_end_time.seconds_until >= time:
                        future_time = (war_end_time.time - timedelta(seconds=time)).replace(tzinfo=pend.UTC)
                        acceptable_times.append((future_time, r_time))
                set_times = await db_client.reminders.distinct("time", filter={"$and": [{"clan": clan_tag}, {"type": "War"}]})
                if acceptable_times:
                    for future_time, r_time in acceptable_times:
                        if future_time in set_times:
                            loop.call_later((future_time - pend.now(tz=pend.UTC)).total_seconds(), send_reminder,  r_time, war_unique_id, clan_tag, producer)

            await schedule_reminders(reminder_times=reminder_times, war_end_time=war.end_time)
            continue

        if war == previous_war:
            continue

        WAR_CACHE[war_unique_id] = war

        league_group = None
        if war.is_cwl:
            league_group: coc.ClanWarLeagueGroup | None = war.league_group
            league_group = league_group._raw_data

        if previous_war.attacks:
            new_attacks = (a for a in war.attacks if a not in set(previous_war.attacks))
        else:
            new_attacks = war.attacks

        for attack in new_attacks:
            json_data = {"type": "new_attack", "war": war._raw_data, "league_group": league_group,
                         "attack": attack._raw_data, "clan_tag": clan_tag}
            producer.send("war", ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))

        previous_league_group = None
        if previous_war.is_cwl:
            previous_league_group: coc.ClanWarLeagueGroup | None = previous_war.league_group
            previous_league_group = previous_league_group._raw_data

        # NEW WAR
        if previous_war.preparation_start_time and war.preparation_start_time and previous_war.preparation_start_time.time != war.preparation_start_time.time:
            if previous_war.state != "warEnded":
                json_data = {"type": "war_ended", "war": previous_war._raw_data, "league_group": previous_league_group, "clan_tag": clan_tag}
                producer.send("war", ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))
            json_data = {"type": "new_war", "war": war._raw_data, "league_group": league_group, "clan_tag": clan_tag}
            producer.send("war", ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))
        elif war.preparation_start_time and not previous_war.preparation_start_time:
            json_data = {"type": "new_war", "war": war._raw_data, "league_group": league_group, "clan_tag": clan_tag}
            producer.send("war", ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))

        if war.state != previous_war.state and previous_war.state == "warEnded":
            json_data = {"type": "war_ended", "war": previous_war._raw_data, "league_group": previous_league_group, "clan_tag": clan_tag}
            producer.send("war", ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))



async def raid_weekend_track(clan_tags: List[str], db_client: MongoDatabase, coc_client: coc.Client, producer: KafkaProducer):
    cached_raids = await db_client.capital_cache.find({"tag": {"$in": clan_tags}}).to_list(length=None)
    cached_raids = {r.get("tag"): r.get("data") for r in cached_raids}

    tasks = []
    for clan_tag in clan_tags:
        tasks.append(coc_client.get_raid_log(clan_tag=clan_tag, limit=1))
    current_raids = await asyncio.gather(*tasks, return_exceptions=True)

    current_raids: Dict[str, coc.RaidLogEntry] = {r[0].clan_tag : r[0] for r in current_raids if r and not isinstance(r, BaseException)}


    db_updates = []
    for clan_tag in clan_tags:
        current_raid = current_raids.get(clan_tag)
        previous_raid = cached_raids.get(clan_tag)

        if current_raid is None:
            continue

        if current_raid._raw_data != previous_raid:
            db_updates.append(UpdateOne({"tag" : clan_tag}, {"$set" : {"data" : current_raid._raw_data}}, upsert=True))

            if previous_raid is None:
                continue

            previous_raid = coc.RaidLogEntry(data=previous_raid, client=coc_client, clan_tag=clan_tag)

            if previous_raid.attack_log:
                new_clans = (clan for clan in current_raid.attack_log if clan not in previous_raid.attack_log)
            else:
                new_clans = current_raid.attack_log

            for clan in new_clans:
                json_data = {"type": "new_offensive_opponent", "clan": clan._raw_data, "clan_tag": clan_tag, "raid": current_raid._raw_data}
                producer.send("capital", ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))

            attacked = []
            for member in current_raid.members:
                old_member = coc.utils.get(previous_raid.members, tag=member.tag)
                if old_member is None or (old_member.attack_count != member.attack_count):
                    attacked.append(member.tag)

            json_data = {"type": "raid_attacks",
                         "clan_tag": current_raid.clan_tag,
                         "attacked" : attacked,
                         "raid": current_raid._raw_data}
            producer.send("capital", ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))

            if current_raid.state != previous_raid.state:
                json_data = {"type": "raid_state", "clan_tag": current_raid.clan_tag, "old_raid": previous_raid._raw_data, "raid": current_raid._raw_data}
                producer.send("capital", ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))


    await db_client.capital_cache.bulk_write(db_updates, ordered=False)



async def clan_track(clan_tag: str, coc_client: coc.Client, producer: KafkaProducer):
    try:
        clan = await coc_client.get_clan(tag=clan_tag)
    except Exception:
        return

    previous_clan: coc.Clan = CLAN_CACHE.get(clan.tag)
    CLAN_CACHE[clan.tag] = clan

    if previous_clan is None:
        return None

    attributes = ["level", "type", "description", "location", "capital_league", "required_townhall", "required_trophies", "war_win_streak", "war_league", "member_count"]
    for attribute in attributes:
        if getattr(clan, attribute) != getattr(previous_clan, attribute):
            json_data = {"type": attribute, "old_clan": previous_clan._raw_data, "new_clan": clan._raw_data}
            producer.send("clan", value=ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))
    current_tags = set(n.tag for n in previous_clan.members)
    if current_tags:
        # we can't check the member_count first incase 1 person left and joined within the 60sec.
        members_joined = (n for n in clan.members if n.tag not in current_tags)
        for member in members_joined:
            json_data = {"type": "member_join", "clan": clan._raw_data, "member": member._raw_data}
            producer.send("clan", ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))

    current_tags = set(n.tag for n in clan.members)
    if current_tags:
        members_left = (n for n in previous_clan.members if n.tag not in current_tags)
        for member in members_left:
            json_data = {"type": "member_leave", "clan": clan._raw_data, "member": member._raw_data}
            producer.send("clan", ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))

    previous_donations = {n.tag: (n.donations, n.received) for n in previous_clan.members}
    for member in clan.members:
        if (member_donated := previous_donations.get(member.tag)) is not None:
            mem_donos, mem_received = member_donated
            if mem_donos < member.donations or mem_received < member.received:
                json_data = {"type": "all_member_donations", "old_clan": previous_clan._raw_data, "new_clan": clan._raw_data}
                producer.send("clan", ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))
                break


