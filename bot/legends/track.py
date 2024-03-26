
import pendulum as pend
import orjson
import time
import snappy

from kafka import KafkaProducer
from collections import deque
from redis import asyncio as redis
from loguru import logger
from utility.keycreation import create_keys
from pymongo import UpdateOne, InsertOne
from utility.classes import MongoDatabase
from .config import LegendTrackingConfig
from .utils import get_player_responses
from collections import defaultdict
from utility.utils import gen_season_date, gen_raid_date, gen_games_season, gen_legend_date


async def main():
    config = LegendTrackingConfig()

    producer = KafkaProducer(bootstrap_servers=["85.10.200.219:9092"], api_version=(3, 6, 0))
    db_client = MongoDatabase(stats_db_connection=config.stats_mongodb, static_db_connection=config.static_mongodb)

    cache = redis.Redis(host=config.redis_ip, port=6379, db=10, password=config.redis_pw, decode_responses=False, max_connections=50,
                        health_check_interval=10, socket_connect_timeout=5, retry_on_timeout=True, socket_keepalive=True)
    keys: deque = await create_keys([config.coc_email.format(x=x) for x in range(config.min_coc_email, config.max_coc_email + 1)], [config.coc_password] * config.max_coc_email)
    logger.info(f"{len(keys)} keys created")

    loop_spot = 0

    while True:
        loop_spot += 1
        time_inside = time.time()

        all_tags_to_track = await db_client.player_stats.distinct("tag", filter={"league" : "Legend League"})
        logger.info(f"{len(all_tags_to_track)} players to track")

        split_size = 50_000
        split_tags = [all_tags_to_track[i:i + split_size] for i in range(0, len(all_tags_to_track), split_size)]

        legend_changes = []
        clan_tags: set = set(await db_client.clans_db.distinct("tag"))

        for count, group in enumerate(split_tags, 1):
            # update last updated for all the members we are checking this go around
            logger.info(f"LOOP {loop_spot} | Group {count}/{len(split_tags)}: {len(group)} tags")

            # pull previous responses from cache + map to a dict so we can easily pull
            previous_player_responses = await cache.mget(keys=group)
            previous_player_responses = {tag: response for tag, response in zip(group, previous_player_responses)}

            # pull current responses from the api, returns (tag: str, response: bytes)
            # response can be bytes, "delete", and None
            current_player_responses = await get_player_responses(keys=keys, tags=group)

            logger.info(f"LOOP {loop_spot} | Group {count}: Entering Changes Loop/Pulled Responses")
            pipe = cache.pipeline()

            legend_date = gen_legend_date()
            for tag, response in current_player_responses:

                previous_response = previous_player_responses.get(tag)
                if response is None:
                    continue

                if response == "delete":
                    await db_client.player_stats.delete_one({"tag" : tag})
                    await pipe.getdel(tag)
                    continue

                # if None, update cache and move on
                if previous_response is None:
                    await pipe.set(tag, response, ex=2_592_000)
                    continue

                # if the responses don't match:
                # - update cache
                # - turn responses into dicts
                # - use function to find changes & update lists of changes
                if previous_response != response:
                    await pipe.set(tag, response, ex=2_592_000)
                    response = orjson.loads(response)
                    previous_response = orjson.loads(previous_response)

                    tag = response.get("tag")
                    league = response.get("league", {}).get("name", "Unranked")

                    if response["trophies"] != previous_response["trophies"] and response["trophies"] >= 4900 and league == "Legend League":
                        diff_trophies = response["trophies"] - previous_response["trophies"]
                        diff_attacks = response["attackWins"] - previous_response["attackWins"]

                        json_data = {"types": ["legends"], "old_player": previous_response, "new_player": response, "timestamp": int(pend.now(tz=pend.UTC).timestamp())}
                        if response.get("clan", {}).get("tag", "None") in clan_tags:
                            producer.send(topic="player", value=orjson.dumps(json_data), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp()) * 1000)

                        if diff_trophies <= - 1:
                            diff_trophies = abs(diff_trophies)
                            if diff_trophies <= 100:
                                legend_changes.append(UpdateOne({"tag": tag},
                                                                 {"$push": {f"legends.{legend_date}.defenses": diff_trophies}}, upsert=True))

                                legend_changes.append(UpdateOne({"tag": tag},
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

                            legend_changes.append(
                                UpdateOne({"tag": tag},
                                          {"$inc": {f"legends.{legend_date}.num_attacks": diff_attacks}},
                                          upsert=True))
                            # if one attack
                            if diff_attacks == 1:
                                legend_changes.append(UpdateOne({"tag": tag},
                                                                 {"$push": {
                                                                     f"legends.{legend_date}.attacks": diff_trophies}},
                                                                 upsert=True))
                                legend_changes.append(UpdateOne({"tag": tag},
                                                                 {"$push": {f"legends.{legend_date}.new_attacks": {
                                                                     "change": diff_trophies,
                                                                     "time": int(pend.now(tz=pend.UTC).timestamp()),
                                                                     "trophies": response["trophies"],
                                                                     "hero_gear": equipment
                                                                 }}}, upsert=True))
                                if diff_trophies == 40:
                                    legend_changes.append(
                                        UpdateOne({"tag": tag}, {"$inc": {f"legends.streak": 1}}))

                                else:
                                    legend_changes.append(
                                        UpdateOne({"tag": tag}, {"$set": {f"legends.streak": 0}}))

                            # if multiple attacks, but divisible by 40
                            elif diff_attacks > 1 and diff_trophies / 40 == diff_attacks:
                                for x in range(0, diff_attacks):
                                    legend_changes.append(
                                        UpdateOne({"tag": tag},
                                                  {"$push": {f"legends.{legend_date}.attacks": 40}},
                                                  upsert=True))
                                    legend_changes.append(UpdateOne({"tag": tag},
                                                                     {"$push": {f"legends.{legend_date}.new_attacks": {
                                                                         "change": 40,
                                                                         "time": int(pend.now(tz=pend.UTC).timestamp()),
                                                                         "trophies": response["trophies"],
                                                                         "hero_gear": equipment
                                                                     }}}, upsert=True))
                                legend_changes.append(
                                    UpdateOne({"tag": tag}, {"$inc": {f"legends.streak": diff_attacks}}))
                            else:
                                legend_changes.append(UpdateOne({"tag": tag},
                                                                 {"$push": {
                                                                     f"legends.{legend_date}.attacks": diff_trophies}},
                                                                 upsert=True))
                                legend_changes.append(UpdateOne({"tag": tag},
                                                                 {"$push": {f"legends.{legend_date}.new_attacks": {
                                                                     "change": diff_trophies,
                                                                     "time": int(pend.now(tz=pend.UTC).timestamp()),
                                                                     "trophies": response["trophies"],
                                                                     "hero_gear": equipment
                                                                 }}}, upsert=True))

                                legend_changes.append(
                                    UpdateOne({"tag": tag}, {"$set": {f"legends.streak": 0}}, upsert=True))

                        if response["defenseWins"] != previous_response["defenseWins"]:
                            diff_defenses = response["defenseWins"] - previous_response["defenseWins"]
                            for x in range(0, diff_defenses):
                                legend_changes.append(
                                    UpdateOne({"tag": tag}, {"$push": {f"legends.{legend_date}.defenses": 0}},
                                              upsert=True))
                                legend_changes.append(UpdateOne({"tag": tag},
                                                                 {"$push": {f"legends.{legend_date}.new_defenses": {
                                                                     "change": 0,
                                                                     "time": int(pend.now(tz=pend.UTC).timestamp()),
                                                                     "trophies": response["trophies"]
                                                                 }}}, upsert=True))

            await pipe.execute()
            logger.info(f"LOOP {loop_spot}: Changes Found")


        logger.info(f"{len(legend_changes)} db changes")
        if legend_changes:
            await db_client.player_stats.bulk_write(legend_changes)
            logger.info(f"STAT CHANGES INSERT: {time.time() - time_inside}")



