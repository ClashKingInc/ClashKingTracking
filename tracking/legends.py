import asyncio
from typing import List

import aiohttp
import coc
import pendulum as pend
from loguru import logger
from pymongo import UpdateOne

from tracking import Tracking
from utility.config import TrackingType
from utility.time import gen_legend_date


class LegendTracking(Tracking):
    def __init__(self):
        super().__init__(tracker_type=TrackingType.BOT_LEGENDS, batch_size=25_000)

        self.cache: dict[str, coc.ClanMember] = {}

        self.player_tags: list = []
        self.bot_clan_tags: set = set()
        self.other_clan_tags: set = set()
        self.split_size: int = 50_000
        self._running: bool = True
        self.legend_date: str = gen_legend_date()

        self.fields: list = [
            "name",
            "tag",
            "trophies",
            "attackWins",
            "defenseWins",
            "heroes",
            "heroEquipment",
            "clan",
            "league",
        ]

        self.db_changes: list = []

    def _player_tags(self):
        pipeline = [
            {"$match": {"league": "Legend League"}},
            {"$project": {"tag": 1, "_id": 0}},
            {"$group": {"_id": "$tag"}},
        ]
        self.player_tags = [
            x["_id"]
            for x in self.mongo.player_stats.aggregate(pipeline, hint={"league": 1, "tag": 1}).to_list(length=None)
        ]
        return

    def _clan_tags(self):
        self.bot_clan_tags = set(self.mongo.clans_db.distinct("tag"))
        return

    async def update_tags_loop(self):
        while self._running:
            self._player_tags()
            self._clan_tags()
            await asyncio.sleep(120)

    def start_background_update(self):
        asyncio.create_task(self.update_tags_loop())

    def stop_background_update(self):
        self._running = False

    def remove_old_tags(self):
        tag_set = set(self.player_tags)
        keys_to_remove = [key for key in self.cache if key not in tag_set]
        for key in keys_to_remove:
            del self.cache[key]

    def _create_defense_update(self, change, player):
        return [
            UpdateOne({"tag": player.tag}, {"$push": {f"legends.{self.legend_date}.defenses": change}}, upsert=True),
            UpdateOne(
                {"tag": player.tag},
                {
                    "$push": {
                        f"legends.{self.legend_date}.new_defenses": {
                            "change": change,
                            "time": int(pend.now(tz=pend.UTC).timestamp()),
                            "trophies": player.trophies,
                        }
                    }
                },
                upsert=True,
            ),
        ]

    def _create_attack_update(self, change, player, equipment):
        return [
            UpdateOne({"tag": player.tag}, {"$push": {f"legends.{self.legend_date}.attacks": change}}, upsert=True),
            UpdateOne(
                {"tag": player.tag},
                {
                    "$push": {
                        f"legends.{self.legend_date}.new_attacks": {
                            "change": change,
                            "time": int(pend.now(tz=pend.UTC).timestamp()),
                            "trophies": player.trophies,
                            "hero_gear": equipment,
                        }
                    }
                },
                upsert=True,
            ),
        ]

    def compare_players(self, player: str):
        previous_player: Player = self.cache.get(player.tag)

        if previous_player is None:
            self.cache[player.tag] = player
            return

        if previous_player.trophies == player.trophies:
            return

        legend_date = self.get_legend_date()
        self.cache[player.tag] = player

        if player.trophies <= 4900 and player.league.name != "Legend League":
            return

        json_data = {
            "types": ["legends"],
            "old_data": previous_player.raw_data,
            "new_data": player.raw_data,
            "timestamp": int(pend.now(tz=pend.UTC).timestamp()),
        }

        if player.clan.tag in self.clan_tags:
            self.producer.send(
                topic="player",
                value=orjson.dumps(json_data),
                key=player.clan.tag.encode("utf-8"),
                timestamp_ms=int(pend.now(tz=pend.UTC).timestamp()) * 1000,
            )

        trophy_change = player.trophies - previous_player.trophies
        attack_change = player.attackWins - previous_player.attackWins

        # Handle trophy changes
        if trophy_change < 0:
            trophy_change = abs(trophy_change)
            if trophy_change <= 100:
                self.db_changes.extend(self.create_defense_update(trophy_change, player))

        elif trophy_change > 0:
            equipment = [{"name": gear.name, "level": gear.level} for hero in player.heroes for gear in hero.equipment]
            self.db_changes.append(
                UpdateOne(
                    {"tag": player.tag}, {"$inc": {f"legends.{legend_date}.num_attacks": attack_change}}, upsert=True
                )
            )
            if attack_change == 1:
                self.db_changes.extend(self.create_attack_update(trophy_change, player, equipment))
                streak_update = (
                    {"$inc": {"legends.streak": 1}} if trophy_change == 40 else {"$set": {"legends.streak": 0}}
                )
                self.db_changes.append(UpdateOne({"tag": player.tag}, streak_update))

            elif attack_change > 1 and trophy_change == attack_change * 40:
                for _ in range(attack_change):
                    self.db_changes.extend(self.create_attack_update(40, player, equipment))
                self.db_changes.append(UpdateOne({"tag": player.tag}, {"$inc": {"legends.streak": attack_change}}))

            else:
                self.db_changes.extend(self.create_attack_update(trophy_change, player, equipment))
                self.db_changes.append(UpdateOne({"tag": player.tag}, {"$set": {"legends.streak": 0}}, upsert=True))

        # Handle defense wins
        if player.defenseWins != previous_player.defenseWins:
            diff_defenses = player.defenseWins - previous_player.defenseWins
            for _ in range(diff_defenses):
                self.db_changes.extend(self.create_defense_update(0, player))

    async def insert_db_changes(self):
        logger.info(f"{len(self.db_changes)} db changes")
        if self.db_changes:
            await self.db_client.player_stats.bulk_write(self.db_changes)

    async def _get_legend_clan_members(self, clan_tags: list[str]):
        tasks = []
        for tag in clan_tags:
            tasks.append(
                self.fetch(url=f"https://api.clashofclans.com/v1/clans/{tag.replace('#', '%23')}", tag=tag, json=True)
            )
        clan_data: list[tuple[dict, str]] = await self._run_tasks(tasks=tasks, return_exceptions=True, wrapped=True)

        logger.info(f"FETCHED {len(clan_data)} clan data")
        legend_members = {}
        for clan, clan_tag in clan_data:
            if clan is None:
                continue

            for count, member in enumerate(clan["memberList"]):
                if member["league"]["name"] != "Legend League":
                    if count == 0 and clan_tag in self.other_clan_tags:
                        self.other_clan_tags.remove(clan_tag)
                    break
                legend_members[member["tag"]] = {
                    "name": member["name"],
                    "trophies": member["trophies"],
                    "league": member["league"]["name"],
                }
        return legend_members

    async def _get_legend_non_clan_members(self, tags: list[str]):
        tasks = []
        for tag in tags:
            tasks.append(
                self.fetch(url=f"https://api.clashofclans.com/v1/players/{tag.replace('#', '%23')}", tag=tag, json=True)
            )
        player_data: list[tuple[dict, str]] = await self._batch_tasks(tasks=tasks, return_results=True)

        player_map = {}
        for player, tag in player_data:
            if player is None:
                continue
            if "league" not in player or player["league"]["name"] != "Legend League":
                self.player_tags.remove(tag)
                continue
            player_map[tag] = {
                "name": player["name"],
                "trophies": player["trophies"],
                "league": player["league"]["name"],
                "clan": player["clan"]["tag"] if "clan" in player else None,
            }
        return player_map

    async def run(self):
        await self.initialize()
        self.start_background_update()

        self._clan_tags()
        logger.info("Clan Tags loaded")

        self._player_tags()
        logger.info("Player Tags loaded")

        while not self.bot_clan_tags:
            logger.info("Waiting on tags to load, sleeping 5 seconds")
            await asyncio.sleep(5)
            continue

        while True:
            self.remove_old_tags()
            logger.info(f"{len(self.player_tags)} players to track")
            all_tags_to_find = set(self.player_tags)

            legend_clan_members = await self._get_legend_clan_members(
                clan_tags=list(self.other_clan_tags | self.bot_clan_tags)
            )
            logger.info(f"{len(legend_clan_members)} clan legend members to track")

            non_clan_legend_member_tags = [tag for tag in all_tags_to_find if tag not in legend_clan_members]
            logger.info(f"{len(non_clan_legend_member_tags)} non clan legend members to track")

            non_clan_legend_members = await self._get_legend_non_clan_members(tags=non_clan_legend_member_tags)
            logger.info(f"Fetched {len(non_clan_legend_members)}non clan legend members")

            clan_tags = []
            for tag, legend_player in (legend_clan_members | non_clan_legend_members).items():
                if legend_player.get("clan"):
                    clan_tags.append(legend_player["clan"])


            self.other_clan_tags = set(clan_tags) | self.other_clan_tags

            logger.info(f"{len(clan_tags)} clans to track")


asyncio.run(LegendTracking().run())
