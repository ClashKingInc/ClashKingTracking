import asyncio
import itertools
from collections import deque
from typing import List

import aiohttp
import orjson
import pendulum as pend
from kafka import KafkaProducer
from loguru import logger
from pymongo import UpdateOne

from utility.classes import MongoDatabase
from utility.keycreation import create_keys
from utility.utils import gen_legend_date

from .classes import Player
from .config import LegendTrackingConfig


class Tracker:
    def __init__(self, config: LegendTrackingConfig):
        self.cache: dict[str, Player] = {}
        self.producer = KafkaProducer(
            bootstrap_servers=['85.10.200.219:9092'], api_version=(3, 6, 0)
        )
        self.db_client = MongoDatabase(
            stats_db_connection=config.stats_mongodb,
            static_db_connection=config.static_mongodb,
        )
        self.keys: deque = asyncio.get_event_loop().run_until_complete(
            create_keys(
                [
                    config.coc_email.format(x=x)
                    for x in range(
                        config.min_coc_email, config.max_coc_email + 1
                    )
                ],
                [config.coc_password] * config.max_coc_email,
            )
        )
        self.tracked_tags: list = []
        self.clan_tags: set = set()
        self.split_size: int = 50_000
        self._running: bool = True
        self.legend_date: str = gen_legend_date()

        self.fields: list = [
            'name',
            'tag',
            'trophies',
            'attackWins',
            'defenseWins',
            'heroes',
            'heroEquipment',
            'clan',
            'league',
        ]

        self.db_changes: list = []

    async def fetch_tags_to_track(self):
        self.tracked_tags = await self.db_client.player_stats.distinct(
            'tag', filter={'league': 'Legend League'}
        )
        self.clan_tags = set(await self.db_client.clans_db.distinct('tag'))

    async def update_tags_loop(self):
        while self._running:
            await self.fetch_tags_to_track()
            await asyncio.sleep(120)

    def start_background_update(self):
        asyncio.create_task(self.update_tags_loop())

    def stop_background_update(self):
        self._running = False

    async def remove_old_tags(self):
        tag_set = set(self.tracked_tags)
        keys_to_remove = [key for key in self.cache if key not in tag_set]
        for key in keys_to_remove:
            del self.cache[key]

    def split_tags(self) -> list[list[str]]:
        return [
            self.tracked_tags[i : i + self.split_size]
            for i in range(0, len(self.tracked_tags), self.split_size)
        ]

    async def fetch(
        self, url: str, session: aiohttp.ClientSession, headers: dict
    ) -> tuple[str, str | None | dict]:
        async with session.get(url, headers=headers) as response:
            tag = f'#{url.split("%23")[-1]}'
            if response.status == 404:  # remove banned players
                return tag, 'delete'
            elif response.status != 200:
                return tag, None
            new_response = await response.read()
            response = orjson.loads(new_response)
            return tag, {key: response.get(key) for key in self.fields}

    async def get_player_responses(
        self, tags: List[str]
    ) -> List[tuple[str, str | None | dict]]:
        connector = aiohttp.TCPConnector(limit=1200, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=1800)
        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as session:
            tasks = []
            for tag in tags:
                self.keys.rotate(1)  # Rotate keys for each request
                api_key = self.keys[0]
                url = f'https://api.clashofclans.com/v1/players/{tag.replace("#", "%23")}'
                headers = {'Authorization': f'Bearer {api_key}'}
                tasks.append(self.fetch(url, session, headers))
            results = await asyncio.gather(*tasks, return_exceptions=True)
        return results

    def get_legend_date(self):
        self.legend_date = gen_legend_date()
        return self.legend_date

    def create_defense_update(self, change, player):
        return [
            UpdateOne(
                {'tag': player.tag},
                {'$push': {f'legends.{self.legend_date}.defenses': change}},
                upsert=True,
            ),
            UpdateOne(
                {'tag': player.tag},
                {
                    '$push': {
                        f'legends.{self.legend_date}.new_defenses': {
                            'change': change,
                            'time': int(pend.now(tz=pend.UTC).timestamp()),
                            'trophies': player.trophies,
                        }
                    }
                },
                upsert=True,
            ),
        ]

    def create_attack_update(self, change, player, equipment):
        return [
            UpdateOne(
                {'tag': player.tag},
                {'$push': {f'legends.{self.legend_date}.attacks': change}},
                upsert=True,
            ),
            UpdateOne(
                {'tag': player.tag},
                {
                    '$push': {
                        f'legends.{self.legend_date}.new_attacks': {
                            'change': change,
                            'time': int(pend.now(tz=pend.UTC).timestamp()),
                            'trophies': player.trophies,
                            'hero_gear': equipment,
                        }
                    }
                },
                upsert=True,
            ),
        ]

    def compare_players(self, player: Player):
        previous_player: Player = self.cache.get(player.tag)

        if previous_player is None:
            self.cache[player.tag] = player
            return

        if previous_player.trophies == player.trophies:
            return

        legend_date = self.get_legend_date()
        self.cache[player.tag] = player

        if player.trophies <= 4900 and player.league.name != 'Legend League':
            return

        json_data = {
            'types': ['legends'],
            'old_data': previous_player.raw_data,
            'new_data': player.raw_data,
            'timestamp': int(pend.now(tz=pend.UTC).timestamp()),
        }

        if player.clan.tag in self.clan_tags:
            self.producer.send(
                topic='player',
                value=orjson.dumps(json_data),
                key=player.clan.tag.encode('utf-8'),
                timestamp_ms=int(pend.now(tz=pend.UTC).timestamp()) * 1000,
            )

        trophy_change = player.trophies - previous_player.trophies
        attack_change = player.attackWins - previous_player.attackWins

        # Handle trophy changes
        if trophy_change < 0:
            trophy_change = abs(trophy_change)
            if trophy_change <= 100:
                self.db_changes.extend(
                    self.create_defense_update(trophy_change, player)
                )

        elif trophy_change > 0:
            equipment = [
                {'name': gear.name, 'level': gear.level}
                for hero in player.heroes
                for gear in hero.equipment
            ]
            self.db_changes.append(
                UpdateOne(
                    {'tag': player.tag},
                    {
                        '$inc': {
                            f'legends.{legend_date}.num_attacks': attack_change
                        }
                    },
                    upsert=True,
                )
            )
            if attack_change == 1:
                self.db_changes.extend(
                    self.create_attack_update(trophy_change, player, equipment)
                )
                streak_update = (
                    {'$inc': {f'legends.streak': 1}}
                    if trophy_change == 40
                    else {'$set': {f'legends.streak': 0}}
                )
                self.db_changes.append(
                    UpdateOne({'tag': player.tag}, streak_update)
                )

            elif attack_change > 1 and trophy_change == attack_change * 40:
                for _ in range(attack_change):
                    self.db_changes.extend(
                        self.create_attack_update(40, player, equipment)
                    )
                self.db_changes.append(
                    UpdateOne(
                        {'tag': player.tag},
                        {'$inc': {f'legends.streak': attack_change}},
                    )
                )

            else:
                self.db_changes.extend(
                    self.create_attack_update(trophy_change, player, equipment)
                )
                self.db_changes.append(
                    UpdateOne(
                        {'tag': player.tag},
                        {'$set': {f'legends.streak': 0}},
                        upsert=True,
                    )
                )

        # Handle defense wins
        if player.defenseWins != previous_player.defenseWins:
            diff_defenses = player.defenseWins - previous_player.defenseWins
            for _ in range(diff_defenses):
                self.db_changes.extend(self.create_defense_update(0, player))

    async def insert_db_changes(self):
        logger.info(f'{len(self.db_changes)} db changes')
        if self.db_changes:
            await self.db_client.player_stats.bulk_write(self.db_changes)


async def main():

    tracker = Tracker(config=LegendTrackingConfig())

    tracker.start_background_update()

    while not tracker.clan_tags:
        logger.info(f'Waiting on tags to load, sleeping 5 seconds')
        await asyncio.sleep(5)
        continue

    for loop_count in itertools.count():
        try:
            await tracker.remove_old_tags()
            logger.info(f'{len(tracker.tracked_tags)} players to track')

            for count, group in enumerate(tracker.split_tags(), 1):
                logger.info(
                    f'LOOP {loop_count} | Group {count}/{len(tracker.split_tags())}: {len(group)} tags'
                )
                current_player_responses = await tracker.get_player_responses(
                    tags=group
                )
                logger.info(
                    f'LOOP {loop_count} | Group {count}: Pulled Responses'
                )

                for tag, response in current_player_responses:
                    if response is None:
                        continue

                    if response == 'delete':
                        await tracker.db_client.player_stats.delete_one(
                            {'tag': tag}
                        )
                        tracker.cache.pop(tag, 'gone')
                        continue

                    player = Player(raw_data=response)
                    tracker.compare_players(player=player)
        except:
            continue
