
import pendulum as pend
import orjson
import time

from kafka import KafkaProducer
from collections import deque
from loguru import logger
from utility.keycreation import create_keys
from pymongo import UpdateOne
from utility.classes import MongoDatabase
from .config import LegendTrackingConfig
from .utils import get_player_responses
from utility.utils import  gen_legend_date
from msgspec.json import decode
from msgspec import Struct
from typing import Optional, List

class Badges(Struct):
    large: str
    def to_dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}

class Clan(Struct):
    name: str
    tag: str
    badgeUrls: Badges

    def to_dict(self):
        result = {f: getattr(self, f) for f in self.__struct_fields__}
        for field, value in result.items():
            if isinstance(value, Struct):
                result[field] = value.to_dict()  # Recursively convert nested structs
            elif isinstance(value, list) and all(isinstance(item, Struct) for item in value):
                result[field] = [item.to_dict() for item in value]  # Convert lists of structs

        return result

class Equipment(Struct):
    name: str
    level: int

    def to_dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}

class Heroes(Struct):
    name: str
    equipment: Optional[List[Equipment]] = list()

    def to_dict(self):
        result = {f: getattr(self, f) for f in self.__struct_fields__}
        for field, value in result.items():
            if isinstance(value, Struct):
                result[field] = value.to_dict()  # Recursively convert nested structs
            elif isinstance(value, list) and all(isinstance(item, Struct) for item in value):
                result[field] = [item.to_dict() for item in value]  # Convert lists of structs

        return result

class League(Struct):
    name: str

    def to_dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}

class Player(Struct):
    name: str
    tag: str
    trophies: int
    attackWins: int
    defenseWins: int
    heroes: List[Heroes]
    equipment: Optional[List[Equipment]] = list()
    clan: Optional[Clan] = None
    league: Optional[League] = None

    def to_dict(self):
        result = {f: getattr(self, f) for f in self.__struct_fields__}
        for field, value in result.items():
            if isinstance(value, Struct):
                result[field] = value.to_dict()  # Recursively convert nested structs
            elif isinstance(value, list) and all(isinstance(item, Struct) for item in value):
                result[field] = [item.to_dict() for item in value]  # Convert lists of structs

        return result


async def main():
    LEGENDS_CACHE = {}
    config = LegendTrackingConfig()

    producer = KafkaProducer(bootstrap_servers=["85.10.200.219:9092"], api_version=(3, 6, 0))
    db_client = MongoDatabase(stats_db_connection=config.stats_mongodb, static_db_connection=config.static_mongodb)

    keys: deque = await create_keys([config.coc_email.format(x=x) for x in range(config.min_coc_email, config.max_coc_email + 1)], [config.coc_password] * config.max_coc_email)
    logger.info(f"{len(keys)} keys created")

    loop_spot = 0

    while True:

        try:
            loop_spot += 1
            time_inside = time.time()

            all_tags_to_track = await db_client.player_stats.distinct("tag", filter={"league" : "Legend League"})
            tag_set = set(all_tags_to_track)
            keys_to_remove = [key for key in LEGENDS_CACHE if key not in tag_set]
            for key in keys_to_remove:
                del LEGENDS_CACHE[key]

            logger.info(f"{len(all_tags_to_track)} players to track")

            split_size = 50_000
            split_tags = [all_tags_to_track[i:i + split_size] for i in range(0, len(all_tags_to_track), split_size)]

            legend_changes = []
            clan_tags: set = set(await db_client.clans_db.distinct("tag"))

            for count, group in enumerate(split_tags, 1):
                # update last updated for all the members we are checking this go around
                logger.info(f"LOOP {loop_spot} | Group {count}/{len(split_tags)}: {len(group)} tags")

                # pull current responses from the api, returns (tag: str, response: bytes)
                # response can be bytes, "delete", and None
                current_player_responses = await get_player_responses(keys=keys, tags=group)

                logger.info(f"LOOP {loop_spot} | Group {count}: Entering Changes Loop/Pulled Responses")

                legend_date = gen_legend_date()
                for tag, response in current_player_responses:

                    if response is None:
                        continue

                    if response == "delete":
                        await db_client.player_stats.delete_one({"tag" : tag})
                        LEGENDS_CACHE.pop(tag, "gone")
                        continue

                    player = decode(response, type=Player)
                    # if None, update cache and move on
                    previous_player: Player = LEGENDS_CACHE.get(tag)
                    if previous_player is None:
                        LEGENDS_CACHE[tag] = player
                        continue

                    # if the responses don't match:
                    # - update cache
                    if previous_player.trophies != player.trophies:
                        LEGENDS_CACHE[tag] = player

                        if player.league is None:
                            continue

                        if player.trophies >= 4900 and player.league.name == "Legend League":
                            json_data = {"types": ["legends"],
                                         "old_data" : previous_player.to_dict(),
                                         "new_data" : player.to_dict(),
                                         "timestamp": int(pend.now(tz=pend.UTC).timestamp())}
                            if player.clan is not None and player.clan.tag in clan_tags:
                                producer.send(topic="player", value=orjson.dumps(json_data), key=player.clan.tag.encode("utf-8"), timestamp_ms=int(pend.now(tz=pend.UTC).timestamp()) * 1000)

                            diff_trophies = player.trophies - previous_player.trophies
                            diff_attacks = player.attackWins - previous_player.attackWins

                            if diff_trophies <= - 1:
                                diff_trophies = abs(diff_trophies)
                                if diff_trophies <= 100:
                                    legend_changes.append(UpdateOne({"tag": tag},
                                                                     {"$push": {f"legends.{legend_date}.defenses": diff_trophies}}, upsert=True))

                                    legend_changes.append(UpdateOne({"tag": tag},
                                                                     {"$push": {f"legends.{legend_date}.new_defenses": {
                                                                         "change": diff_trophies,
                                                                         "time": int(pend.now(tz=pend.UTC).timestamp()),
                                                                         "trophies": player.trophies
                                                                     }}}, upsert=True))

                            elif diff_trophies >= 1:
                                equipment = []
                                for hero in player.heroes:
                                    for gear in hero.equipment:
                                        equipment.append({"name": gear.name, "level": gear.level})

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
                                                                         "trophies": player.trophies,
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
                                                                             "trophies": player.trophies,
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
                                                                         "trophies": player.trophies,
                                                                         "hero_gear": equipment
                                                                     }}}, upsert=True))

                                    legend_changes.append(
                                        UpdateOne({"tag": tag}, {"$set": {f"legends.streak": 0}}, upsert=True))

                            if player.defenseWins != previous_player.defenseWins:
                                diff_defenses = player.defenseWins - previous_player.defenseWins
                                for x in range(0, diff_defenses):
                                    legend_changes.append(
                                        UpdateOne({"tag": tag}, {"$push": {f"legends.{legend_date}.defenses": 0}},
                                                  upsert=True))
                                    legend_changes.append(UpdateOne({"tag": tag},
                                                                     {"$push": {f"legends.{legend_date}.new_defenses": {
                                                                         "change": 0,
                                                                         "time": int(pend.now(tz=pend.UTC).timestamp()),
                                                                         "trophies": player.trophies
                                                                     }}}, upsert=True))

                logger.info(f"LOOP {loop_spot}: Changes Found")


            logger.info(f"{len(legend_changes)} db changes")
            if legend_changes:
                await db_client.player_stats.bulk_write(legend_changes)
                logger.info(f"STAT CHANGES INSERT: {time.time() - time_inside}")

        except:
            continue



