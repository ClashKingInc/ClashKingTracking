import pendulum as pend
import asyncio
import coc
import ujson

from datetime import timedelta
from kafka import KafkaProducer
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from typing import List, Dict
from pymongo import UpdateOne
from utility.classes import MongoDatabase
from expiring_dict import ExpiringDict

WAR_CACHE = ExpiringDict()
CLAN_CACHE = {}

reminder_times = [f"{int(time)}hr" if time.is_integer() else f"{time}hr" for time in (x * 0.25 for x in range(1, 193))]


def send_reminder(time: str, war_unique_id: str, clan_tag: str, producer: KafkaProducer):
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


ONLY_KEEP = None
async def clan_war_track(clan_tag: str, db_client: MongoDatabase, coc_client: coc.Client, producer: KafkaProducer, scheduler: AsyncIOScheduler):
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

        war_unique_id = "-".join([war.clan_tag, war.opponent.tag]) + f"-{int(war.preparation_start_time.time.timestamp())}"

        previous_war = WAR_CACHE.get(war_unique_id)
        if previous_war is None:
            WAR_CACHE[war_unique_id] = war

            acceptable_times = []
            for r_time in reversed(reminder_times):
                time = r_time.replace("hr", "")
                time_seconds = int(float(time) * 3600)
                if war.end_time.seconds_until >= time_seconds:
                    future_time = (war.end_time.time.replace(tzinfo=pend.UTC) - timedelta(seconds=time_seconds))
                    acceptable_times.append((future_time, f"{time} hr"))
            set_times = await db_client.reminders.distinct("time", filter={"$and": [{"clan": clan_tag}, {"type": "War"}]})
            if acceptable_times:
                for future_time, r_time in acceptable_times:
                    if r_time in set_times:
                        scheduler.add_job(send_reminder, 'date', run_date=future_time, args=[r_time, war_unique_id, clan_tag, producer],
                                          id=f"war_end_{war.clan.tag}_{war.opponent.tag}_{future_time.timestamp()}", misfire_grace_time=1200, max_instances=1)
            continue

        if war._raw_data == previous_war._raw_data:
            continue

        WAR_CACHE[war_unique_id] = war

        league_group = None
        if war.is_cwl:
            league_group: coc.ClanWarLeagueGroup | None = war.league_group
            league_group = league_group._raw_data

        if previous_war.attacks:
            new_attacks = [a for a in war.attacks if a not in set(previous_war.attacks)]
        else:
            new_attacks = war.attacks

        if new_attacks:
            json_data = {"type": "new_attacks", "war": war._raw_data, "league_group": league_group,
                         "attacks": [attack._raw_data for attack in new_attacks], "clan_tag": clan_tag}
            producer.send("war", ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))

        previous_league_group = None
        if previous_war.is_cwl:
            previous_league_group: coc.ClanWarLeagueGroup | None = previous_war.league_group
            previous_league_group = previous_league_group._raw_data

        if war.state != previous_war.state:
            json_data = {"type": "war_state", "old_war" : previous_war._raw_data, "new_war": war._raw_data, "previous_league_group": previous_league_group,
                         "new_league_group" : league_group, "clan_tag": clan_tag}
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

            clan_data = CLAN_CACHE.get(clan_tag)

            if clan_data:
                json_data = {"type": "raid_attacks",
                             "clan_tag": current_raid.clan_tag,
                             "attacked" : attacked,
                             "raid": current_raid._raw_data,
                             "old_raid" : previous_raid._raw_data,
                             "clan" : clan_data._raw_data
                             }
                producer.send("capital", ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))

            if current_raid.state != previous_raid.state:
                json_data = {"type": "raid_state", "clan_tag": current_raid.clan_tag, "old_raid": previous_raid._raw_data, "raid": current_raid._raw_data}
                producer.send("capital", ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))

    if db_updates:
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
    changed_attributes = []
    for attribute in attributes:
        if getattr(clan, attribute) != getattr(previous_clan, attribute):
            changed_attributes.append(attribute)
    if changed_attributes:
        json_data = {"types": changed_attributes, "old_clan": previous_clan._raw_data, "new_clan": clan._raw_data}
        producer.send("clan", value=ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))


    members_joined = [n._raw_data for n in clan.members if n.tag not in set(n.tag for n in previous_clan.members)]
    members_left = [n._raw_data for n in previous_clan.members if n.tag not in set(n.tag for n in clan.members)]
    if members_joined or members_left:
        json_data = {"type": "members_join_leave", "old_clan": previous_clan._raw_data, "new_clan": clan._raw_data, "joined": members_joined, "left" : members_left}
        producer.send("clan", ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))

    previous_donations = {n.tag: (n.donations, n.received) for n in previous_clan.members}
    for member in clan.members:
        if (member_donated := previous_donations.get(member.tag)) is not None:
            mem_donos, mem_received = member_donated
            if mem_donos < member.donations or mem_received < member.received:
                json_data = {"type": "all_member_donations", "old_clan": previous_clan._raw_data, "new_clan": clan._raw_data}
                producer.send("clan", ujson.dumps(json_data).encode("utf-8"), key=clan_tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))
                break


