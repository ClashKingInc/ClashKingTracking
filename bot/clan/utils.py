import pendulum as pend
import asyncio
import coc
import ujson

from datetime import timedelta
from kafka import KafkaProducer
from utility.http import HTTPClient, Route
from typing import List
from loguru import logger
from collections import deque
from utility.classes import MongoDatabase
from expiring_dict import ExpiringDict

WAR_CACHE = ExpiringDict()

#WAR UTILS
async def get_war(http_client: HTTPClient, db_client: MongoDatabase, clan_tag: str, headers: dict):

    clan_tag_for_url = clan_tag.replace('#', '%23')
    war_response = await http_client.request(Route("GET", f"/clans/{clan_tag_for_url}/currentwar"), headers=headers)
    if war_response is None:
        now = pend.now(tz=pend.UTC).now()
        result = await db_client.clan_wars.find_one({"$and": [{"endTime": {"$gte": now}}, {"opponent": clan_tag}]})
        if result is not None:
            war_response = await http_client.request(Route("GET", f"/clans/{clan_tag_for_url}/currentwar"), headers=headers)
            if war_response is not None:
                return_value = (None, war_response)
            else:
                return_value = (None, None)
        else:
            return_value = (None, None)
    else:
        return_value = (None, war_response)

    if return_value == (None, None) or (return_value[1] is not None and return_value[1].get("state") == "notInWar"):
        league_group_response = await http_client.request(Route("GET", f"/clans/{clan_tag_for_url}/currentwar/leaguegroup"), headers=headers)
        if league_group_response is None:
            return_value = (None, None)
        else:
            return_value = (True, True)

    current_round = next_round = None
    if return_value == (True, True):
        rounds: list = league_group_response.get("rounds")
        current_round_war_tags = []
        next_round_war_tags = []
        for count, round in enumerate(reversed(rounds), 1):
            current_round_war_tags = round.get("warTags")
            if "#0" in current_round_war_tags:
                continue
            current_round = -(count)
        if current_round is None:
            return_value = (None, None)
        else:
            num_rounds = len(rounds)
            if current_round + 1 == 0:  # we are on last round
                next_round = None
            else:
                next_round = rounds[current_round + 1]
                next_round_war_tags = next_round.get("warTags")
                if "#0" in next_round_war_tags:
                    next_round = None

        if current_round is not None:
            for war_tag in current_round_war_tags:
                league_war_response = await http_client.request(Route("GET", f"/clanwarleagues/wars/{war_tag.replace('#', '%23')}"), headers=headers)
                if league_war_response is not None:
                    if league_war_response.get("clan").get("tag") == clan_tag or league_war_response.get("opponent").get("tag") == clan_tag:
                        current_round = league_war_response
                        break

        if next_round is not None:
            for war_tag in next_round_war_tags:
                league_war_response = await http_client.request(Route("GET", f"/clanwarleagues/wars/{war_tag.replace('#', '%23')}"), headers=headers)
                if league_war_response is not None:
                    if league_war_response.get("clan").get("tag") == clan_tag or league_war_response.get("opponent").get("tag") == clan_tag:
                        next_round = league_war_response
                        break

    if current_round is None and next_round is None:
        return [clan_tag, return_value]
    else:
        return [clan_tag, (next_round, current_round)]


async def get_war_responses(http_client: HTTPClient, db_client: MongoDatabase, keys: deque, clan_tags: List[str]):
    war_tasks = []
    for tag in clan_tags:
        headers = {"Authorization": f"Bearer {keys[0]}"}
        keys.rotate(1)
        war_tasks.append(get_war(http_client=http_client, db_client=db_client, clan_tag=tag, headers=headers))

    war_responses = await asyncio.gather(*war_tasks)
    logger.info(f"{len(clan_tags)} clan tags | {len(war_responses)} war responses")
    return war_responses

def send_reminder(acceptable_times: list[tuple], time: str, war_unique_id: str, clan_tag: str ,producer: KafkaProducer):
    war_data = WAR_CACHE.get(war_unique_id)
    if war_data is None:
        return

    if acceptable_times:
        top_time = acceptable_times.pop(0)
        future_time, r_time = top_time
        asyncio.get_event_loop().call_later((future_time - pend.now(tz=pend.UTC)).total_seconds(), send_reminder, acceptable_times, r_time, war_unique_id, clan_tag, producer)

    json_data = {
        "type" : "reminder",
        "clan_tag" : clan_tag,
        "time" : time,
        "data" : war_data
    }
    producer.send("reminder", ujson.dumps(json_data).encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))


async def handle_war_responses(coc_client: coc.Client, producer: KafkaProducer, responses):
    loop = asyncio.get_event_loop()
    reminder_times = [f"{int(time)}hr" if time.is_integer() else f"{time}hr" for time in (x * 0.25 for x in range(1, 193))]

    for response in responses:
        if isinstance(response, Exception):
            continue
        clan_tag = response[0]
        next_war, current_war = response[1]
        wars = [next_war, current_war]
        for war in wars:
            if war is None:
                continue
            war = coc.ClanWar(data=war, client=coc_client, clan_tag=clan_tag)

            league_group = None
            if war.is_cwl:
                league_group: coc.ClanWarLeagueGroup = war.league_group
                league_group = league_group._raw_data

            war_unique_id = "-".join(sorted([war.clan_tag, war.opponent.tag])) + f"-{int(war.preparation_start_time.time.timestamp())}"

            previous_war_data = WAR_CACHE.get(war_unique_id)
            if previous_war_data is None:
                WAR_CACHE[war_unique_id] = war._raw_data
                def schedule_reminders(reminder_times, war_end_time: coc.Timestamp):
                    acceptable_times = []
                    for r_time in reversed(reminder_times):
                        time = r_time.replace("hr", "")
                        time = int(float(time) * 3600)
                        if war_end_time.seconds_until >= time:
                            future_time = (war_end_time.time - timedelta(seconds=time)).replace(tzinfo=pend.UTC)
                            acceptable_times.append((future_time, r_time))
                    if acceptable_times:
                        top_time = acceptable_times.pop(0)
                        future_time, r_time = top_time
                        loop.call_later((future_time - pend.now(tz=pend.UTC)).total_seconds(), send_reminder, acceptable_times, r_time, war_unique_id, clan_tag, producer)

                schedule_reminders(reminder_times=reminder_times, war_end_time=war.end_time)
                continue

            previous_war: coc.ClanWar = coc.ClanWar(data=previous_war_data, client=coc_client)

            if previous_war.attacks:
                new_attacks = (a for a in war.attacks if a not in set(previous_war.attacks))
            else:
                new_attacks = war.attacks

            for attack in new_attacks:
                json_data = {"type": "new_attack", "war": war._raw_data, "league_group": league_group,
                             "attack": attack._raw_data, "clan_tag": clan_tag}
                producer.send("war", ujson.dumps(json_data).encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))

            previous_league_group = None
            if previous_war.is_cwl:
                previous_league_group: coc.ClanWarLeagueGroup = previous_war.league_group
                previous_league_group = previous_league_group._raw_data

            # NEW WAR
            if previous_war.preparation_start_time and war.preparation_start_time and previous_war.preparation_start_time.time != war.preparation_start_time.time:
                if previous_war.state != "warEnded":
                    json_data = {"type": "war_ended", "war": previous_war._raw_data, "league_group": previous_league_group, "clan_tag": clan_tag}
                    producer.send("war", ujson.dumps(json_data).encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))
                json_data = {"type": "new_war", "war": war._raw_data, "league_group": league_group, "clan_tag": clan_tag}
                producer.send("war", ujson.dumps(json_data).encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))
            elif war.preparation_start_time and not previous_war.preparation_start_time:
                json_data = {"type": "new_war", "war": war._raw_data, "league_group": league_group, "clan_tag": clan_tag}
                producer.send("war", ujson.dumps(json_data).encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))

            if war.state != previous_war.state and previous_war.state == "warEnded":
                json_data = {"type": "war_ended", "war": previous_war._raw_data, "league_group": previous_league_group, "clan_tag": clan_tag}
                producer.send("war", ujson.dumps(json_data).encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000))