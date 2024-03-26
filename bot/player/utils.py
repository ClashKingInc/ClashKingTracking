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
from asyncio_throttle import Throttler


async def get_clan_member_tags(db_client: MongoDatabase, keys: deque):
    clan_tags = await db_client.clans_db.distinct("tag")

    tasks = []
    connector = aiohttp.TCPConnector(limit=250)
    async with aiohttp.ClientSession(connector=connector) as session:
        for tag in clan_tags:
            headers = {"Authorization": f"Bearer {keys[0]}"}
            tag = tag.replace("#", "%23")
            url = f"https://api.clashofclans.com/v1/clans/{tag}"
            keys.rotate(1)

            async def fetch(url, session, headers):
                async with session.get(url, headers=headers) as response:
                    try:
                        clan = await response.json()
                        return clan
                    except:
                        return None

            tasks.append(fetch(url, session, headers))
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        await session.close()

    CLAN_MEMBERS = []
    for response in responses:
        try:
            CLAN_MEMBERS += [member["tag"] for member in response["memberList"]]
        except Exception:
            pass


    return CLAN_MEMBERS


async def get_player_responses(keys: deque, tags: list[str]):
    throttler = Throttler(rate_limit=1200, period=1)
    tasks = []
    connector = aiohttp.TCPConnector(limit=500, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=1800)
    session = aiohttp.ClientSession(connector=connector, timeout=timeout)
    for tag in tags:
        keys.rotate(1)

        async def fetch(url, session: aiohttp.ClientSession, headers, throttler: Throttler):
            async with throttler:
                async with session.get(url, headers=headers) as response:
                    t = f'#{url.split("%23")[-1]}'
                    if response.status == 404:  # remove banned players
                        return (t, "delete")
                    elif response.status != 200:
                        return (t, None)
                    new_response = await response.read()
                    return (t, new_response)

        tasks.append(
            fetch(url=f'https://api.clashofclans.com/v1/players/{tag.replace("#", "%23")}', session=session, headers={"Authorization": f"Bearer {keys[0]}"}, throttler=throttler))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    await session.close()
    return results


def get_player_changes(previous_response: dict, response: dict):
    new_json = {}
    fields_to_update = []
    ok_achievements = {"Gold Grab", "Elixir Escapade", "Heroic Heist", "Games Champion", "Aggressive Capitalism",
                       "Well Seasoned", "Nice and Tidy", "War League Legend", "Wall Buster"}
    for key, item in response.items():
        old_item = previous_response.get(key)
        if old_item != item:
            fields_to_update.append(key)
        not_ok_fields = {"labels", "legendStatistics", "playerHouse", "versusBattleWinCount"}
        if key in not_ok_fields:
            continue
        if old_item != item:
            if isinstance(item, list):
                for count, spot in enumerate(item):
                    spot_name = spot["name"]
                    if key == "achievements" and spot_name not in ok_achievements:
                        continue
                    old_ = next((item for item in old_item if item["name"] == spot_name), None)
                    if old_ != spot:
                        if key == "achievements":
                            if old_ is not None:
                                new_json[(key, spot_name.replace(".", ""))] = (old_["value"], spot["value"])
                            else:
                                new_json[(key, spot_name.replace(".", ""))] = (None, spot["value"])
                        else:
                            if old_ is not None:
                                new_json[(key, spot_name.replace(".", ""))] = (old_["level"], spot["level"])
                            else:
                                new_json[(key, spot_name.replace(".", ""))] = (None, spot["level"])
            else:
                if key == "clan":
                    new_json[(key, key)] = (None, {"tag": item["tag"], "name": item["name"]})
                elif key == "league":
                    new_json[(key, key)] = (None, {"tag": item["id"], "name": item["name"]})
                else:
                    new_json[(key, key)] = (old_item, item)

    return (new_json, fields_to_update)


def find_and_list_changes(producer: KafkaProducer, response: dict, previous_response: dict, bulk_db_changes: list, auto_complete: list, bulk_insert: list, bulk_clan_changes: list):
    BEEN_ONLINE = False
    start = time.time()

    season = gen_season_date()
    raid_date = gen_raid_date()
    games_season = gen_games_season()
    legend_date = gen_legend_date()

    tag = response.get("tag")
    clan_tag = response.get("clan", {}).get("tag", "Unknown")
    league = response.get("league", {}).get("name", "Unranked")
    prev_league = previous_response.get("league", {}).get("name", "Unranked")
    if league != prev_league:
        bulk_db_changes.append(UpdateOne({"tag": tag}, {"$set": {"league": league}}))
        auto_complete.append(UpdateOne({"tag": tag}, {"$set": {"league": league}}))

    changes, fields_to_update = get_player_changes(previous_response, response)

    online_types = {"donations", "Gold Grab", "Most Valuable Clanmate", "attackWins",
                    "War League Legend",
                    "Wall Buster", "name", "Well Seasoned", "Games Champion", "Elixir Escapade",
                    "Heroic Heist",
                    "warPreference", "warStars", "Nice and Tidy", "builderBaseTrophies"}
    skip_store_types = {"War League Legend", "Wall Buster", "Aggressive Capitalism",
                        "Baby Dragon",
                        "Elixir Escapade",
                        "Gold Grab", "Heroic Heist", "Nice and Tidy", "Well Seasoned",
                        "attackWins",
                        "builderBaseTrophies", "donations", "donationsReceived", "trophies",
                        "versusBattleWins", "versusTrophies"}

    special_types = {"War League Legend", "warStars", "Aggressive Capitalism", "Nice and Tidy", "Well Seasoned",
                     "clanCapitalContributions", "Games Champion"}
    ws_types = {"clanCapitalContributions", "name", "troops", "heroes", "spells", "heroEquipment",
                "townHallLevel",
                "league", "trophies", "Most Valuable Clanmate"}
    only_once = {"troops": 0, "heroes": 0, "spells": 0, "heroEquipment": 0}
    if changes:
        def recursive_defaultdict():
            return defaultdict(recursive_defaultdict)

        player_level_changes = defaultdict(recursive_defaultdict)
        clan_level_changes = defaultdict(recursive_defaultdict)

        type_changes = []
        for (parent, type_), (old_value, value) in changes.items():
            if type_ in special_types:
                bulk_db_changes.append(UpdateOne({"tag": tag},
                                                 {"$set": {f"{type_.replace(' ', '_').lower()}": value}},
                                                 upsert=True))

            if type_ not in skip_store_types:
                if old_value is None:
                    bulk_insert.append(InsertOne({"tag": tag, "type": type_, "value": value,
                                                  "time": int(pend.now(tz=pend.UTC).timestamp()),
                                                  "clan": clan_tag, "th": response.get("townHallLevel")}))
                else:
                    bulk_insert.append(InsertOne(
                        {"tag": tag, "type": type_, "p_value": old_value, "value": value,
                         "time": int(pend.now(tz=pend.UTC).timestamp()), "clan": clan_tag, "th": response.get("townHallLevel")}))

            if type_ == "donations":
                previous_dono = 0 if (previous_dono := previous_response["donations"]) > (current_dono := response["donations"]) else previous_dono
                player_level_changes["$inc"][f"donations.{season}.donated"] = (current_dono - previous_dono)
                clan_level_changes["$inc"][f"{season}.{tag}.donations"] = (current_dono - previous_dono)

            elif type_ == "donationsReceived":
                previous_dono = 0 if (previous_dono := previous_response["donationsReceived"]) > (current_dono := response["donationsReceived"]) else previous_dono
                player_level_changes["$inc"][f"donations.{season}.received"] = (current_dono - previous_dono)
                clan_level_changes["$inc"][f"{season}.{tag}.received"] = (current_dono - previous_dono)


            elif type_ == "clanCapitalContributions":
                player_level_changes["$push"][f"capital_gold.{raid_date}.donate"] = (response["clanCapitalContributions"] - previous_response["clanCapitalContributions"])
                clan_level_changes["$inc"][f"{season}.{tag}.capital_gold_dono"] = (response["clanCapitalContributions"] - previous_response["clanCapitalContributions"])
                type_ = "Most Valuable Clanmate"  # temporary

            elif type_ == "Gold Grab":
                diff = value - old_value
                player_level_changes["$inc"][f"gold.{season}"] = diff
                clan_level_changes["$inc"][f"{season}.{tag}.gold_looted"] = diff

            elif type_ == "Elixir Escapade":
                diff = value - old_value
                player_level_changes["$inc"][f"elixir.{season}"] = diff
                clan_level_changes["$inc"][f"{season}.{tag}.elixir_looted"] = diff

            elif type_ == "Heroic Heist":
                diff = value - old_value
                player_level_changes["$inc"][f"dark_elixir.{season}"] = diff
                clan_level_changes["$inc"][f"{season}.{tag}.dark_elixir_looted"] = diff

            elif type_ == "Well Seasoned":
                diff = value - old_value
                player_level_changes["$inc"][f"season_pass.{games_season}"] = diff

            elif type_ == "Games Champion":
                diff = value - old_value
                player_level_changes["$inc"][f"clan_games.{games_season}.points"] = diff
                player_level_changes["$set"][f"clan_games.{games_season}.clan"] = clan_tag
                clan_level_changes["$inc"][f"{games_season}.{tag}.clan_games"] = diff

            elif type_ == "attackWins":
                player_level_changes["$set"][f"attack_wins.{season}"] = value
                clan_level_changes["$set"][f"{season}.{tag}.attack_wins"] = value

            elif type_ == "trophies":
                player_level_changes["$set"][f"season_trophies.{season}"] = value
                clan_level_changes["$set"][f"{season}.{tag}.trophies"] = value

            elif type_ == "name":
                player_level_changes["$set"]["name"] = value
                auto_complete.append(UpdateOne({"tag": tag}, {"$set": {"name": value}}))

            elif type_ == "clan":
                player_level_changes["$set"]["clan_tag"] = clan_tag
                auto_complete.append(UpdateOne({"tag": tag}, {"$set": {"clan": clan_tag}}))

            elif type_ == "townHallLevel":
                player_level_changes["$set"]["townhall"] = value
                auto_complete.append(UpdateOne({"tag": tag}, {"$set": {"th": value}}))

            elif parent in {"troops", "heroes", "spells", "heroEquipment"}:
                type_ = parent
                if only_once[parent] == 1:
                    continue
                only_once[parent] += 1

            if type_ in online_types:
                BEEN_ONLINE = True

            if type_ in ws_types:
                if type_ == "trophies":
                    if not (value >= 4900 and league == "Legend League"):
                        continue
                type_changes.append(type_)

        if type_changes:
            json_data = {"types": type_changes, "old_player": previous_response,
                         "new_player": response, "timestamp": int(pend.now(tz=pend.UTC).timestamp())}
            producer.send(topic="player", value=orjson.dumps(json_data), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp()) * 1000)

        def to_regular_dict(d):
            """Recursively converts a defaultdict to a regular dict."""
            if isinstance(d, defaultdict):
                # Convert the defaultdict to dict
                d = {key: to_regular_dict(value) for key, value in d.items()}
            return d

        if player_level_changes:
            bulk_db_changes.append(UpdateOne(
                {"tag": tag},
                to_regular_dict(player_level_changes),
                upsert=True
            ))

        if clan_level_changes:
            clan_level_changes["$set"][f"{season}.{tag}.name"] = response.get("name")
            clan_level_changes["$set"][f"{season}.{tag}.townhall"] = response.get("townHallLevel")
            bulk_clan_changes.append(UpdateOne(
                {"tag": clan_tag},
                to_regular_dict(clan_level_changes),
                upsert=True
            ))

    # LEGENDS CODE, dont fix what aint broke

    if response["trophies"] != previous_response["trophies"] and response["trophies"] >= 4900 and league == "Legend League":
        diff_trophies = response["trophies"] - previous_response["trophies"]
        diff_attacks = response["attackWins"] - previous_response["attackWins"]

        if diff_trophies <= - 1:
            diff_trophies = abs(diff_trophies)
            if diff_trophies <= 100:
                bulk_db_changes.append(UpdateOne({"tag": tag},
                                                 {"$push": {f"legends.{legend_date}.defenses": diff_trophies}}, upsert=True))

                bulk_db_changes.append(UpdateOne({"tag": tag},
                                                 {"$push": {f"legends.{legend_date}.new_defenses": {
                                                     "change": diff_trophies,
                                                     "time": int(pend.now(tz=pend.UTC).timestamp()),
                                                     "trophies": response["trophies"]
                                                 }}}, upsert=True))

        elif diff_trophies >= 1:
            heroes = response.get("heroes", [])
            equipment = []
            for hero in heroes:
                for gear in hero.get("equipment", []):
                    equipment.append({"name": gear.get("name"), "level": gear.get("level")})

            bulk_db_changes.append(
                UpdateOne({"tag": tag},
                          {"$inc": {f"legends.{legend_date}.num_attacks": diff_attacks}},
                          upsert=True))
            # if one attack
            if diff_attacks == 1:
                bulk_db_changes.append(UpdateOne({"tag": tag},
                                                 {"$push": {
                                                     f"legends.{legend_date}.attacks": diff_trophies}},
                                                 upsert=True))
                bulk_db_changes.append(UpdateOne({"tag": tag},
                                                 {"$push": {f"legends.{legend_date}.new_attacks": {
                                                     "change": diff_trophies,
                                                     "time": int(pend.now(tz=pend.UTC).timestamp()),
                                                     "trophies": response["trophies"],
                                                     "hero_gear": equipment
                                                 }}}, upsert=True))
                if diff_trophies == 40:
                    bulk_db_changes.append(
                        UpdateOne({"tag": tag}, {"$inc": {f"legends.streak": 1}}))

                else:
                    bulk_db_changes.append(
                        UpdateOne({"tag": tag}, {"$set": {f"legends.streak": 0}}))

            # if multiple attacks, but divisible by 40
            elif diff_attacks > 1 and diff_trophies / 40 == diff_attacks:
                for x in range(0, diff_attacks):
                    bulk_db_changes.append(
                        UpdateOne({"tag": tag},
                                  {"$push": {f"legends.{legend_date}.attacks": 40}},
                                  upsert=True))
                    bulk_db_changes.append(UpdateOne({"tag": tag},
                                                     {"$push": {f"legends.{legend_date}.new_attacks": {
                                                         "change": 40,
                                                         "time": int(pend.now(tz=pend.UTC).timestamp()),
                                                         "trophies": response["trophies"],
                                                         "hero_gear": equipment
                                                     }}}, upsert=True))
                bulk_db_changes.append(
                    UpdateOne({"tag": tag}, {"$inc": {f"legends.streak": diff_attacks}}))
            else:
                bulk_db_changes.append(UpdateOne({"tag": tag},
                                                 {"$push": {
                                                     f"legends.{legend_date}.attacks": diff_trophies}},
                                                 upsert=True))
                bulk_db_changes.append(UpdateOne({"tag": tag},
                                                 {"$push": {f"legends.{legend_date}.new_attacks": {
                                                     "change": diff_trophies,
                                                     "time": int(pend.now(tz=pend.UTC).timestamp()),
                                                     "trophies": response["trophies"],
                                                     "hero_gear": equipment
                                                 }}}, upsert=True))

                bulk_db_changes.append(
                    UpdateOne({"tag": tag}, {"$set": {f"legends.streak": 0}}, upsert=True))

        if response["defenseWins"] != previous_response["defenseWins"]:
            diff_defenses = response["defenseWins"] - previous_response["defenseWins"]
            for x in range(0, diff_defenses):
                bulk_db_changes.append(
                    UpdateOne({"tag": tag}, {"$push": {f"legends.{legend_date}.defenses": 0}},
                              upsert=True))
                bulk_db_changes.append(UpdateOne({"tag": tag},
                                                 {"$push": {f"legends.{legend_date}.new_defenses": {
                                                     "change": 0,
                                                     "time": int(pend.now(tz=pend.UTC).timestamp()),
                                                     "trophies": response["trophies"]
                                                 }}}, upsert=True))


    if BEEN_ONLINE:
        _time = int(pend.now(tz=pend.UTC).timestamp())
        bulk_db_changes.append(
            UpdateOne({"tag": tag}, {
                "$inc": {f"activity.{season}": 1},
                "$push": {f"last_online_times.{season}": _time},
                "$set": {"last_online": _time}
            }, upsert=True))
        bulk_clan_changes.append(
            UpdateOne({"tag": clan_tag},
                      {"$inc": {f"{season}.{tag}.activity": 1}},
                      upsert=True))



async def update_autocomplete(member_tags: list, cached_data: dict, db_client: MongoDatabase):
    #delete anyone from the autocomplete that we don't need
    await db_client.player_search.delete_many({"tag" : {"$nin" : member_tags}})

    class Clan(Struct):
        tag: str
        name: str

    class League(Struct):
        name: str

    class Player(Struct):
        tag: str
        name: str
        townHallLevel: int
        trophies: int
        clan: Optional[Clan] = None
        league: Optional[League] = None


    autocomplete_changes = []
    for tag, player in cached_data.items():
        try:
            player = decode(snappy.decompress(player), type=Player)
        except Exception as e:
            continue
        autocomplete_changes.append(UpdateOne({"tag" : player.tag},
                                              {"$set" :
                                                   {"name": player.name,
                                                    "clan": player.clan.tag if player.clan else "No Clan",
                                                    "clan_name": player.clan.name if player.clan else "No Clan",
                                                    "league": player.league.name if player.league else "Unranked",
                                                    "trophies" : player.trophies,
                                                    "th": player.townHallLevel}}))

    if autocomplete_changes:
        await db_client.player_search.bulk_write(autocomplete_changes, ordered=False)



