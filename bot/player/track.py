import pendulum as pend
import orjson
import time
import snappy

from kafka import KafkaProducer
from collections import deque
from redis import asyncio as redis
from loguru import logger
from .utils import get_clan_member_tags, get_player_responses, find_and_list_changes, update_autocomplete, get_player_changes
from utility.keycreation import create_keys
from pymongo import UpdateOne, InsertOne
from utility.classes import MongoDatabase
from .config import BotPlayerTrackingConfig
from collections import defaultdict
from utility.utils import gen_season_date, gen_raid_date, gen_games_season


async def main():
    config = BotPlayerTrackingConfig()

    producer = KafkaProducer(bootstrap_servers=["85.10.200.219:9092"], api_version=(3, 6, 0))
    db_client = MongoDatabase(stats_db_connection=config.stats_mongodb, static_db_connection=config.static_mongodb)

    cache = redis.Redis(host=config.redis_ip, port=6379, db=0, password=config.redis_pw, decode_responses=False, max_connections=50,
                        health_check_interval=10, socket_connect_timeout=5, retry_on_timeout=True, socket_keepalive=True)
    keys: deque = await create_keys([config.coc_email.format(x=x) for x in range(config.min_coc_email, config.max_coc_email + 1)], [config.coc_password] * config.max_coc_email)
    logger.info(f"{len(keys)} keys created")

    loop_spot = 1

    while True:
            loop_spot += 1
            time_inside = time.time()

            clan_tags: set = set(await db_client.clans_db.distinct("tag"))
            all_tags_to_track = await get_clan_member_tags(db_client=db_client, keys=keys)
            gone_for_a_month = int(pend.now(tz=pend.UTC).timestamp()) - 2_592_000

            # every 100 loops, track the people who have been inactive for more than a month
            # approx 5-6 hours
            if loop_spot % 60 == 0:
                pipeline = [{"$match": {"last_online": {"$gte": gone_for_a_month}}}, {"$project": {"tag": "$tag"}}, {"$unset": "_id"}]
                db_tags = [x["tag"] for x in (await db_client.player_stats.aggregate(pipeline).to_list(length=None))]
                all_tags_to_track = list(set(db_tags + all_tags_to_track))

            # every 20 loops, track the people who have been active in the last 30 days
            # approx every 1-1.5 hours
            if loop_spot % 20 == 0 and loop_spot % 60 != 0:
                pipeline = [{"$match": {"last_online": {"$lte": gone_for_a_month}}}, {"$project": {"tag": "$tag"}}, {"$unset": "_id"}]
                db_tags = [x["tag"] for x in (await db_client.player_stats.aggregate(pipeline).to_list(length=None))]
                all_tags_to_track = list(set(db_tags + all_tags_to_track))

            if loop_spot <= 1:
                pipeline = [{"$match": {}},
                            {"$project": {"tag": "$tag"}},
                            {"$unset": "_id"}]
                db_tags = [x["tag"] for x in (await db_client.player_stats.aggregate(pipeline).to_list(length=None))]
                all_tags_to_track = list(set(db_tags + all_tags_to_track))


            logger.info(f"{len(all_tags_to_track)} players to track")

            # on the first loop, we get everyone tracked

            split_size = 50_000
            split_tags = [all_tags_to_track[i:i + split_size] for i in range(0, len(all_tags_to_track), split_size)]

            bulk_db_changes = []
            bulk_insert = []
            bulk_clan_changes = []

            for count, group in enumerate(split_tags, 1):
                # update last updated for all the members we are checking this go around
                await db_client.player_stats.update_many({"tag": {"$in": group}}, {"$set": {"last_updated": int(pend.now(tz=pend.UTC).timestamp())}})
                logger.info(f"LOOP {loop_spot} | Group {count}/{len(split_tags)}: {len(group)} tags")

                # pull previous responses from cache + map to a dict so we can easily pull
                previous_player_responses = await cache.mget(keys=group)
                previous_player_responses = {tag: response for tag, response in zip(group, previous_player_responses)}

                # pull current responses from the api, returns (tag: str, response: bytes)
                # response can be bytes, "delete", and None
                current_player_responses = await get_player_responses(keys=keys, tags=group)

                logger.info(f"LOOP {loop_spot} | Group {count}: Entering Changes Loop/Pulled Responses")
                pipe = cache.pipeline()

                for tag, response in current_player_responses:

                    season = gen_season_date()
                    raid_date = gen_raid_date()
                    games_season = gen_games_season()

                    previous_response = previous_player_responses.get(tag)
                    if response is None:
                        continue

                    if response == "delete":
                        await db_client.player_stats.delete_one({"tag" : tag})
                        await pipe.getdel(tag)
                        continue

                    # if None, update cache and move on
                    if previous_response is None:
                        response = snappy.compress(response)
                        await pipe.set(tag, response, ex=2_592_000)
                        continue

                    # if the responses don't match:
                    # - update cache
                    # - turn responses into dicts
                    # - use function to find changes & update lists of changes
                    compressed_response = snappy.compress(response)
                    if previous_response != compressed_response:
                        await pipe.set(tag, compressed_response, ex=2_592_000)
                        response = orjson.loads(response)
                        previous_response = orjson.loads(snappy.decompress(previous_response))

                        BEEN_ONLINE = False

                        tag = response.get("tag")
                        clan_tag = response.get("clan", {}).get("tag", "No Clan")
                        league = response.get("league", {}).get("name", "Unranked")
                        prev_league = previous_response.get("league", {}).get("name", "Unranked")
                        if league != prev_league:
                            bulk_db_changes.append(UpdateOne({"tag": tag}, {"$set": {"league": league}}))

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
                                    "league", "Most Valuable Clanmate"}
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
                                    previous_dono = 0 if (previous_dono := previous_response["donationsReceived"]) > (
                                        current_dono := response["donationsReceived"]) else previous_dono
                                    player_level_changes["$inc"][f"donations.{season}.received"] = (current_dono - previous_dono)
                                    clan_level_changes["$inc"][f"{season}.{tag}.received"] = (current_dono - previous_dono)

                                elif type_ == "clanCapitalContributions":
                                    player_level_changes["$push"][f"capital_gold.{raid_date}.donate"] = (
                                                response["clanCapitalContributions"] - previous_response["clanCapitalContributions"])
                                    clan_level_changes["$inc"][f"{season}.{tag}.capital_gold_dono"] = (
                                                response["clanCapitalContributions"] - previous_response["clanCapitalContributions"])
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

                                elif type_ == "clan":
                                    player_level_changes["$set"]["clan_tag"] = clan_tag

                                elif type_ == "townHallLevel":
                                    player_level_changes["$set"]["townhall"] = value

                                elif parent in {"troops", "heroes", "spells", "heroEquipment"}:
                                    type_ = parent
                                    if only_once[parent] == 1:
                                        continue
                                    only_once[parent] += 1

                                if type_ in online_types:
                                    BEEN_ONLINE = True

                                if type_ in ws_types:
                                    type_changes.append(type_)

                            if type_changes and (clan_tag in clan_tags):
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

                            if clan_level_changes and (clan_tag in clan_tags):
                                clan_level_changes["$set"][f"{season}.{tag}.name"] = response.get("name")
                                clan_level_changes["$set"][f"{season}.{tag}.townhall"] = response.get("townHallLevel")
                                bulk_clan_changes.append(UpdateOne(
                                    {"tag": clan_tag},
                                    to_regular_dict(clan_level_changes),
                                    upsert=True
                                ))

                        if BEEN_ONLINE:
                            _time = int(pend.now(tz=pend.UTC).timestamp())
                            bulk_db_changes.append(
                                UpdateOne({"tag": tag}, {
                                    "$inc": {f"activity.{season}": 1},
                                    "$push": {f"last_online_times.{season}": _time},
                                    "$set": {"last_online": _time}
                                }, upsert=True))
                            if clan_tag in clan_tags:
                                bulk_clan_changes.append(
                                    UpdateOne({"tag": clan_tag},
                                              {"$inc": {f"{season}.{tag}.activity": 1}},
                                              upsert=True))

                await pipe.execute()
                logger.info(f"LOOP {loop_spot}: Changes Found")


            logger.info(f"{len(bulk_db_changes)} db changes")
            if bulk_db_changes:
                await db_client.player_stats.bulk_write(bulk_db_changes)
                logger.info(f"STAT CHANGES INSERT: {time.time() - time_inside}")

            if bulk_insert:
                await db_client.player_history.bulk_write(bulk_insert)
                logger.info(f"HISTORY CHANGES INSERT: {time.time() - time_inside}")

            if bulk_clan_changes:
                await db_client.clan_stats.bulk_write(bulk_clan_changes)
                logger.info(f"CLAN CHANGES UPDATE: {time.time() - time_inside}")

            fix_changes = []
            not_set_entirely = await db_client.player_stats.distinct("tag", filter={"$or": [{"name": None}, {"league": None}, {"townhall": None}, {"clan_tag": None}]})
            logger.info(f'{len(not_set_entirely)} tags to fix')
            fix_tag_cache = await cache.mget(keys=not_set_entirely)
            for tag, response in zip(not_set_entirely, fix_tag_cache):
                if response is None:
                    continue
                response = orjson.loads(snappy.decompress(response))
                clan_tag = response.get("clan", {}).get("tag", "Unknown")
                league = response.get("league", {}).get("name", "Unranked")
                fix_changes.append(UpdateOne({"tag": tag}, {"$set": {"name": response.get('name'), "townhall": response.get('townHallLevel'), "league": league, "clan_tag": clan_tag}}))

            if fix_changes:
                await db_client.player_stats.bulk_write(fix_changes, ordered=False)
                logger.info(f"FIX CHANGES: {time.time() - time_inside}")
