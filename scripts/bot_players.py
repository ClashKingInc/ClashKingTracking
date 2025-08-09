import asyncio

import coc
import orjson
import pendulum as pend
import snappy
from pymongo import InsertOne, UpdateOne

from .tracking import Tracking, TrackingType
from utility.time import gen_season_date


class PlayerTracking(Tracking):
    def __init__(self):
        super().__init__(batch_size=25_000, tracker_type=TrackingType.BOT_PLAYER)
        self.tracked_tags = set()

        self.store_types = {
            "name",
            "troops",
            "heroes",
            "spells",
            "heroEquipment",
            "townHallLevel",
            "warStars",
            "warPreference",
            "bestBuilderBaseTrophies",
            "bestTrophies",
            "expLevel"
        }

        self.online_types = {
            "donations",
            "Gold Grab",
            "Most Valuable Clanmate",
            "attackWins",
            "War League Legend",
            "Wall Buster",
            "name",
            "Well Seasoned",
            "Games Champion",
            "Elixir Escapade",
            "Heroic Heist",
            "warPreference",
            "warStars",
            "Nice and Tidy",
            "builderBaseTrophies",
            "Anti-Artillery",
            "Firefighter",
            "X-Bow Exterminator"
        }
        self.ws_types = {
            "clanCapitalContributions",
            "name",
            "troops",
            "heroes",
            "spells",
            "heroEquipment",
            "townHallLevel",
            "league",
            "Most Valuable Clanmate",
            "role"
        }
        self.seasonal_inc = {
            "donations": "donated",
            "donationsReceived": "received",
            "clanCapitalContributions": "capital_gold_dono",
            "Gold Grab": "gold_looted",
            "Elixir Escapade": "elixir_looted",
            "Heroic Heist": "dark_elixir_looted",
            "Well Seasoned": "season_pass",
            "Games Champion": "clan_games",
            "Nice and Tidy" : "obstacles_removed",
            "Superb Work" : "boosted_super_troops",
            "Wall Buster": "walls_destroyed",
        }
        self.seasonal_set = {"attackWins": "attack_wins", "trophies": "trophies"}

        self.season_stats = []
        self.historical_changes = []
        self.last_online = []

        self.last_run = pend.now(tz=pend.UTC)

    def _priority_players(self) -> set[str]:
        return set(self.mongo.user_settings.distinct("search.player.bookmarked"))

    async def _get_clan_member_tags(self) -> list[str]:
        clan_tags = self.mongo.clans_db.distinct("tag")

        tasks = [self.coc_client.get_clan(tag=tag) for tag in clan_tags]
        clans = await self._run_tasks(tasks=tasks, return_exceptions=True, wrapped=True)

        clan_members = []
        for clan in clans:
            if not isinstance(clan, coc.Clan):
                continue
            if clan.members == 0:
                self.mongo.clans_db.delete_many({"tag": clan.tag})
                continue
            clan_members.extend([member.tag for member in clan.members if member.town_hall >= 4])

        clan_members = self._priority_players() | set(clan_members)
        return list(clan_members)

    def _get_cache_tags(self):
        cursor = 0
        keys = []
        while True:
            cursor, batch = self.redis_decoded.scan(cursor=cursor, match="player-cache:*", count=25_000)
            batch = [b.split(":")[-1] for b in batch]
            keys.extend(batch)
            if cursor == 0:
                break
        return set(keys)

    def get_player_changes(self, previous_response: dict, response: dict):
        changes = {}

        not_ok_fields = {"labels", "legendStatistics", "playerHouse", "versusBattleWinCount"}

        for key, value in response.items():
            previous_value = previous_response.get(key)
            # if it is a field we dont check, or is not a list but hasnt changed in value, continue
            if key in not_ok_fields or (not isinstance(value, list) and value == previous_value):
                continue

            if not isinstance(value, list):
                if (key == "league" or key == "builderBaseLeague") and previous_value:
                    changes[key] = (
                        {"tag": previous_value["id"], "name": previous_value["name"]},
                        {"tag": value["id"], "name": value["name"]},
                    )
                else:
                    changes[key] = (previous_value, value)
                continue

            # means that value is a list
            for list_item in value:  # type: dict
                # all list items in clash have a name
                fixed_name = list_item["name"].replace(".", "")

                if fixed_name == "Baby Dragon":
                    if list_item["village"] == "builderBase":
                        fixed_name = "Baby Dragon (Builder Base)"

                old_list_item = next((item for item in previous_value if item["name"] == list_item["name"]), {})

                if key == "heroes":
                    old_list_item.pop('equipment', None)
                    list_item.pop("equipment", None)

                if old_list_item != list_item:
                    if key == "achievements":
                        changes[(key, fixed_name)] = (old_list_item.get("value", 0), list_item["value"])
                    else:
                        changes[(key, fixed_name)] = (old_list_item.get("level", 0), list_item["level"])

        return changes

    async def _track(self, player_tags: list[str]):
        tasks = [
            self.fetch(url=f"https://api.clashofclans.com/v1/players/{tag.replace('#', '%23')}", json=False, tag=tag)
            for tag in player_tags
        ]

        results = await self._run_tasks(tasks=tasks, return_exceptions=True, wrapped=True)

        previous_player_responses = self.redis_raw.mget(keys=[f"player-cache:{tag}" for tag in player_tags])
        previous_player_responses = {tag: response
                                     for tag, response in zip(player_tags, previous_player_responses)}
        pipe = self.redis_raw.pipeline()
        season = gen_season_date()

        for result in results:  # type: bytes, str
            if isinstance(result, coc.NotFound):
                tag = result.__notes__[0]
                # self.mongo.player_stats.delete_one({"tag": tag})
                pipe.getdel(f"player-cache:{tag}")
                continue

            elif isinstance(result, coc.Maintenance):
                break
            elif isinstance(result, coc.ClashOfClansException):
                break

            player_data, tag = result
            previous_response = previous_player_responses.get(tag)
            if not previous_response:
                pipe.set(f"player-cache:{tag}", snappy.compress(player_data))
                continue

            # if the responses don't match:
            # - update cache
            # - turn responses into dicts
            # - use function to find changes & update lists of changes
            compressed_response = snappy.compress(player_data)
            if previous_response == compressed_response:
                continue

            pipe.set(f"player-cache:{tag}", compressed_response)
            response = orjson.loads(player_data)
            previous_response = orjson.loads(snappy.decompress(previous_response))

            player_changes = self.get_player_changes(previous_response=previous_response, response=response)
            clan_tag = response.get("clan", {}).get("tag", None)

            activity_score = 0
            changed_keys = set()
            for key, value in player_changes.items():  # type: tuple[str, str] | str, tuple[Any, Any]
                item_key = key
                if isinstance(key, tuple):
                    key, item_key = key
                old_value, new_value = value

                change = None
                if isinstance(old_value, int) and isinstance(new_value, int):
                    change = new_value - old_value

                if item_key in self.store_types or key in self.store_types:
                    self.historical_changes.append(
                        InsertOne(
                            {
                                "tag": tag,
                                "type": item_key,
                                "p_value": old_value,
                                "value": new_value,
                                "time": int(pend.now(tz=pend.UTC).timestamp()),
                                "clan": clan_tag,
                                "th": response.get("townHallLevel"),
                            }
                        )
                    )

                if item_key in self.online_types or key == "heroEquipment":
                    if activity_score == 0:
                        self.season_stats.append(
                            UpdateOne(
                                {"tag": tag, "season": season, "clan_tag": clan_tag}, {"$inc": {"activity_score": 1}}
                            )
                        )
                    activity_score += 1

                if key in self.seasonal_inc and change > 0 and clan_tag:
                    self.season_stats.append(
                        UpdateOne(
                            {"tag": tag, "season": season, "clan_tag": clan_tag},
                            {"$inc": {self.seasonal_inc.get(key): change}},
                        )
                    )

                if key in self.seasonal_set and clan_tag:
                    self.season_stats.append(
                        UpdateOne(
                            {"tag": tag, "season": season, "clan_tag": clan_tag},
                            {"$set": {self.seasonal_set.get(key): change}},
                        )
                    )

                if key in self.ws_types:
                    changed_keys.add(key)

            if changed_keys:
                json_data = {
                    "types": list(changed_keys),
                    "old_player": previous_response,
                    "new_player": response,
                    "timestamp": int(pend.now(tz=pend.UTC).timestamp()),
                }
                self._send_to_kafka("player", json_data, key=clan_tag)

            if activity_score:
                self.last_online.append(
                    InsertOne({
                        "timestamp": pend.now(tz=pend.UTC),
                        "meta": {
                            "tag": tag,
                            "clan_tag": clan_tag,
                        }
                    })
                )

        self.logger.debug(f"Inserting {len(self.last_online)} last online records")
        if self.last_online:
            self.mongo.last_online.bulk_write(self.last_online, ordered=False)
            self.last_online.clear()

        self.logger.debug(f"Updating {len(self.historical_changes)} historical changes")
        if self.historical_changes:
            self.mongo.player_history.bulk_write(self.historical_changes, ordered=False)
            self.historical_changes.clear()

        self.logger.debug(f"Updating {len(self.season_stats)} season stats")
        if self.season_stats:
            self.mongo.new_player_stats.bulk_write(self.season_stats, ordered=False)
            self.season_stats.clear()

        pipe.execute()

    async def _batch(self, player_tags: list[str]):
        batches = self._split_into_batch(items=player_tags)

        self.logger.info(f"Starting {len(batches)} batches")
        for batch in batches:
            await self._check_maintenance()
            await self._track(player_tags=batch)
        self.logger.info(f"Completed {len(batches)} batches")

    def _clean_cache(self, player_tags: list[str]):
        if not self.tracked_tags:
            self.tracked_tags = self._get_cache_tags()

        player_tags = set(player_tags)
        stale_tags = self.tracked_tags - player_tags

        keys_to_delete = [f"player-cache:{tag}" for tag in stale_tags]

        if keys_to_delete:
            pipe = self.redis_raw.pipeline()
            for key in keys_to_delete:
                pipe.delete(key)
            pipe.execute()

        self.tracked_tags = player_tags

    async def run(self):
        await self.initialize()
        while True:
            player_tags = await self._get_clan_member_tags()
            self._clean_cache(player_tags=player_tags)
            await self._batch(player_tags=player_tags)
            self._submit_stats()

