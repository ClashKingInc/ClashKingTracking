import asyncio
from typing import List

import aiohttp
import coc
import pendulum as pend
from loguru import logger
from mypy.checkexpr import defaultdict
from pymongo import UpdateOne

from tracking import Tracking
from utility.config import TrackingType
from utility.time import gen_legend_date


class LegendTracking(Tracking):
    def __init__(self):
        super().__init__(tracker_type=TrackingType.BOT_LEGENDS, batch_size=25_000)

        self.cache: dict[str, dict] = {}

        self.bot_clan_tags: set = set()
        self.player_tags: set = set()
        self.other_clan_tags: set = set()

        self.split_size: int = 50_000
        self._running: bool = True
        self.legend_date: str = gen_legend_date()

        self.tried_to_find_in_legends = defaultdict(int)
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
        self.tripled_last_time: dict[str, bool] = {}
        self.db_changes: list = []

    def _player_tags(self) -> set[str]:
        return set(self.mongo.base_player.distinct("tag", filter={"legends_tracking": True}))

    def _clan_tags(self):
        return set(self.mongo.clans_db.distinct("tag"))


    async def _get_legend_clan_members_to_check(self) -> set:
        """
        Check legend players and if trophies changed, then they need to be checked
        """

        tasks = []
        all_tags = self.bot_clan_tags | self.other_clan_tags
        for tag in all_tags:
            tasks.append(
                self.fetch(url=f"https://api.clashofclans.com/v1/clans/{tag.replace('#', '%23')}", tag=tag, json=True)
            )
        batches = self._split_into_batch(items=tasks)
        clan_data: list[tuple[dict, str]] = []
        for batch in batches:
            responses = await self._run_tasks(tasks=batch, return_exceptions=True, wrapped=True)
            clan_data.extend(responses)

        self.logger.info(f"FETCHED {len(clan_data)} clan data")

        members_that_changed = set()
        legend_clan_members = set()
        for clan, clan_tag in clan_data:
            if clan is None:
                continue

            for count, member in enumerate(clan["memberList"]):
                if member["league"]["name"] != "Legend League":
                    if count == 0 and clan_tag in self.other_clan_tags:
                        self.other_clan_tags.remove(clan_tag)
                    break
                legend_clan_members.add(member["tag"])
                if member["trophies"] != self.cache.get(member["tag"], {}).get("trophies", 0):
                    members_that_changed.add(member["tag"])
                elif member['tag'] in self.player_tags:
                    self.player_tags.remove(member['tag'])

        #remove any players that arent in our player tag or clan member set from the cache
        full_player_set = self.player_tags | legend_clan_members
        keys_to_remove = [key for key in self.cache if key not in full_player_set]
        for key in keys_to_remove:
            del self.cache[key]

        return members_that_changed


    async def _find_changes(self, tags: list[str]):
        self.logger.info(f"fetching {len(tags)} tags")
        tasks = []
        for tag in tags:
            tasks.append(
                self.fetch(url=f"https://api.clashofclans.com/v1/players/{tag.replace('#', '%23')}", tag=tag, json=True)
            )
        responses: list[tuple[dict, str]] = await self._run_tasks(tasks=tasks, return_exceptions=True, wrapped=True)

        legend_stats = []
        base_player = []

        legend_date = gen_legend_date()
        
        for player_data in responses:
            if isinstance(player_data, coc.NotFound):
                self.mongo.base_player.delete_one({"tag": player_data})
            elif isinstance(player_data, coc.Maintenance):
                break
            elif isinstance(player_data, Exception):
                continue

            player, tag = player_data

            if "league" not in player or player["league"]["name"] != "Legend League":
                self.tried_to_find_in_legends[tag] += 1
                # assuming 3 min tracking, this means they were out of legends for nearly a week, drop their tracking
                if self.tried_to_find_in_legends[tag] >= 3500:
                    self.mongo.base_player.update_one({"tag": tag}, {"$set": {"legends_tracking": False}})
                continue

            player = {k : v for k, v in player.items() if k in self.fields}

            clan_tag = player.get("clan", {}).get("tag")


            previous_player = self.cache.get(tag)
            self.cache[tag] = player
            if previous_player == player or previous_player is None:
                continue

            if clan_tag in self.bot_clan_tags:
                json_data = {
                    "types": ["legends"],
                    "old_data": previous_player,
                    "new_data": player,
                    "timestamp": pend.now(tz=pend.UTC).int_timestamp,
                }
                self._send_to_kafka(topic="player", data=json_data, key=clan_tag)

            trophy_change = player.get("trophies") - previous_player.get("trophies")
            diff_defense = player.get("defenseWins") - previous_player.get("defenseWins")
            attack_change = player.get("attackWins") - previous_player.get("attackWins")

            defenses = []
            if diff_defense:
                defenses.extend([0] * diff_defense)

            if trophy_change < 0:
                defenses.append(abs(trophy_change))

            for defense in defenses:
                if defense <= 100:
                    legend_stats.append(UpdateOne(
                            filter={"tag" : tag, "date" : legend_date},
                            update={
                                "$push": {"defenses": {
                                    "change": defense,
                                    "time": pend.now(tz=pend.UTC).int_timestamp,
                                    "trophies": player.get("trophies"),
                                }},
                                "$inc" : {"defense" : defense, "num_defenses": 1},
                             },
                            upsert=True,
                        ))

            if trophy_change > 0:
                equipment = [{"name": gear.get("name"), "level": gear.get("level")}
                             for hero in player.get("heroes", []) for gear in hero.get("equipment", [])]
                attacks = [attack_change]
                streak = 0
                if trophy_change == attack_change * 40:
                    attacks = [40] * attack_change
                    streak = attack_change

                if streak:
                    base_player.append(UpdateOne(
                        {"tag": tag},
                        {"$inc": {"legends_streak": 1}},
                    ))
                else:
                    base_player.append(UpdateOne(
                        {"tag": tag},
                        {"$set": {"legends_streak": 0}},
                    ))


                for attack in attacks:
                    legend_stats.append(UpdateOne(
                        filter={"tag": tag, "date": legend_date},
                        update={
                            "$push": {"attacks": {
                                "change": attack,
                                "time": pend.now(tz=pend.UTC).int_timestamp,
                                "trophies": player.get("trophies"),
                                "hero_gear": equipment,
                            }},
                            "$inc": {"offense": attack, "num_attacks": 1},
                        },
                        upsert=True,
                    ))

        self.logger.info(f"{len(legend_stats)} legend stats to write")
        if legend_stats:
            self.mongo.new_legend_stats.bulk_write(legend_stats, ordered=False)

        self.logger.info(f"{len(legend_stats)} base player updates")
        if base_player:
            self.mongo.base_player.bulk_write(base_player, ordered=False)
        

    async def _track(self):
        clan_members_changed_trophies = await self._get_legend_clan_members_to_check()
        self.logger.info(f"{len(clan_members_changed_trophies)} clan members changed trophies")

        tags_to_track = self.player_tags | clan_members_changed_trophies
        self.logger.info(f"{len(tags_to_track)} players to track")

        batch = self._split_into_batch(items=list(tags_to_track))
        self.logger.info(f"created {len(batch)} batches ")
        
        for tags in batch:
            await self._find_changes(tags=tags)

        other_clan_tags = set()
        for tag, data in self.cache.items():
            clan_tag = data.get("clan", {}).get("tag")
            if clan_tag and clan_tag not in self.bot_clan_tags:
                other_clan_tags.add(clan_tag)
        self.other_clan_tags = other_clan_tags
        self.logger.info(f"{len(self.other_clan_tags | self.bot_clan_tags)} clan tags to track")


    async def run(self):
        await self.initialize()

        while True:
            self.bot_clan_tags = self._clan_tags()
            self.player_tags = self._player_tags()
            self.logger.info(f"{len(self.player_tags)} players in base player")
            await self._track()





asyncio.run(LegendTracking().run())
