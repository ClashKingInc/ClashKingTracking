import asyncio
import random
import time

import aiohttp
import coc
import orjson
import pendulum as pend
import snappy
import ujson
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from hashids import Hashids
from pymongo import InsertOne, UpdateOne

from utility.config import TrackingType
from utility.constants import locations

"""from .capital_lb import (
    calculate_clan_capital_leaderboards,
    calculate_player_capital_looted_leaderboards,
    calculate_raid_medal_leaderboards,
)
"""


from tracking import Tracking


class ScheduledTracking(Tracking):
    def __init__(
        self,
        tracker_type: TrackingType,
        max_concurrent_requests=1000,
        throttle_speed=1000,
    ):
        super().__init__(
            max_concurrent_requests=max_concurrent_requests,
            tracker_type=tracker_type,
            throttle_speed=throttle_speed,
        )

    def setup_scheduler(self):
        """
        Set up all scheduled jobs with the scheduler.
        """
        self.scheduler.add_job(
            self.store_all_leaderboards,
            CronTrigger(hour=4, minute=56),
            name='Store All Leaderboards',
            misfire_grace_time=300,
        )
        self.scheduler.add_job(
            self.store_legends,
            CronTrigger(day='*', hour=5, minute=56),
            name='Store Legends',
            misfire_grace_time=300,
        )
        self.scheduler.add_job(
            self.store_cwl_wars,
            CronTrigger(day='13', hour=19, minute=37),
            name='Store CWL Wars',
            misfire_grace_time=300,
        )
        self.scheduler.add_job(
            self.store_cwl_groups,
            CronTrigger(day='9-12', hour='*', minute=35),
            name='Store CWL Groups',
            misfire_grace_time=300,
        )
        self.scheduler.add_job(
            self.update_autocomplete,
            IntervalTrigger(minutes=30),
            name='Update Autocomplete',
            misfire_grace_time=300,
        )
        self.scheduler.add_job(
            self.update_region_leaderboards,
            IntervalTrigger(minutes=15),
            name='Update Region Leaderboards',
            misfire_grace_time=300,
        )
        self.scheduler.add_job(
            self.store_clan_capital,
            CronTrigger(day_of_week='mon', hour=10),
            name='Store Clan Capital',
            misfire_grace_time=300,
        )

    async def store_all_leaderboards(self):
        """
        Store all leaderboards data at scheduled times.
        """
        try:
            for database, function in zip(
                [
                    self.db_client.clan_trophies,
                    self.db_client.clan_versus_trophies,
                    self.db_client.capital,
                    self.db_client.player_trophies,
                    self.db_client.player_versus_trophies,
                ],
                [
                    self.coc_client.get_location_clans,
                    self.coc_client.get_location_clans_builder_base,
                    self.coc_client.get_location_clans_capital,
                    self.coc_client.get_location_players,
                    self.coc_client.get_location_players_builder_base,
                ],
            ):

                tasks = [
                    asyncio.create_task(function(location_id=location))
                    for location in locations
                ]

                responses = await asyncio.gather(
                    *tasks, return_exceptions=True
                )
                store_tasks = []
                for index, response in enumerate(responses):
                    if isinstance(response, Exception):
                        self.logger.error(
                            f'Error fetching data for location {locations[index]}: {response}'
                        )
                        continue
                    location = locations[index]
                    store_tasks.append(
                        InsertOne(
                            {
                                'location': location,
                                'date': str(pend.now(tz=pend.UTC).date()),
                                'data': {
                                    'items': [x._raw_data for x in response]
                                },
                            }
                        )
                    )

                if store_tasks:
                    try:
                        await database.bulk_write(store_tasks)
                        self.logger.info(
                            f'Stored leaderboards for locations batch.'
                        )
                    except Exception as e:
                        self.logger.error(f'Error storing leaderboards: {e}')
        except Exception as e:
            self.logger.exception(
                f'Unexpected error in store_all_leaderboards: {e}'
            )

    async def store_legends(self):
        """
        Store legends data at scheduled times.
        """
        try:
            seasons = await self.coc_client.get_seasons(league_id=29000022)

            seasons_present = await self.db_client.legend_history.distinct(
                'season'
            )
            missing = set(seasons) - set(seasons_present)

            for year in missing:
                after = ''
                while after is not None:
                    changes = []
                    async with aiohttp.ClientSession() as session:
                        if after:
                            after_param = f'&after={after}'
                        else:
                            after_param = ''
                        headers = {
                            'Accept': 'application/json',
                            'authorization': f'Bearer {self.keys[0]}',
                        }
                        self.keys.rotate()
                        url = f'https://api.clashofclans.com/v1/leagues/29000022/seasons/{year}?limit=25000{after_param}'
                        async with session.get(
                            url, headers=headers
                        ) as response:
                            if response.status != 200:
                                print(await response.json())
                                self.logger.error(
                                    f'Failed to fetch legends for season {year}: {response.status}'
                                )
                                break
                            items = await response.json()
                            players = items.get('items', [])
                            for player in players:
                                player['season'] = year
                                changes.append(InsertOne(player))
                            after = (
                                items.get('paging', {})
                                .get('cursors', {})
                                .get('after', None)
                            )

                    if changes:
                        try:
                            await self.db_client.legend_history.bulk_write(
                                changes, ordered=False
                            )
                            self.logger.info(
                                f'Inserted {len(changes)} legend records for season {year}'
                            )
                        except Exception as e:
                            self.logger.error(
                                f'Error inserting legends for season {year}: {e}'
                            )
        except Exception as e:
            self.logger.exception(f'Unexpected error in store_legends: {e}')

    async def store_cwl_wars(self):
        """
        Store Clan War League wars data at scheduled times.
        """
        try:
            hashids = Hashids(min_length=7)
            season = self.gen_games_season()
            season = '2024-12'
            pipeline = [
                {'$match': {'data.season': season}},
                {'$group': {'_id': '$data.rounds.warTags'}},
            ]
            result = await self.db_client.cwl_group.aggregate(
                pipeline
            ).to_list(length=None)
            done_for_this_season = [x['_id'] for x in result]
            done_for_this_season = [
                j for sub in done_for_this_season for j in sub
            ]
            all_tags = set([j for sub in done_for_this_season for j in sub])
            self.logger.info(f'{len(all_tags)} war tags total')

            pipeline = [
                {'$match': {'data.season': season}},
                {'$group': {'_id': '$data.tag'}},
            ]
            tags_already_found = set(
                [
                    x['_id']
                    for x in (
                        await self.db_client.clan_wars.aggregate(
                            pipeline
                        ).to_list(length=None)
                    )
                ]
            )
            self.logger.info(
                f'{len(tags_already_found)} war tags already found'
            )
            all_tags = [t for t in all_tags if t not in tags_already_found]

            self.logger.info(f'{len(all_tags)} war tags to find')
            all_tags = [
                all_tags[i : i + self.batch_size]
                for i in range(0, len(all_tags), self.batch_size)
            ]

            for count, tag_group in enumerate(all_tags, 1):
                start_time = time.time()
                self.logger.info(f'GROUP {count} | {len(tag_group)} tags')
                tasks = []
                for tag in tag_group:
                    tasks.append(
                        self.fetch(
                            url=f"https://api.clashofclans.com/v1/clanwarleagues/wars/{tag.replace('#', '%23')}",
                            tag=tag,
                        )
                    )
                    self.logger.info(f'{len(tasks)} tasks')
                    responses = await asyncio.gather(*tasks)

                self.logger.info(
                    f'{len(responses)} responses | {time.time() - start_time} sec'
                )

                add_war = []
                responses = [
                    (orjson.loads(r), tag)
                    for r, tag in responses
                    if r is not None
                ]
                self.logger.info(
                    f'{len(responses)} valid responses | {time.time() - start_time} sec'
                )
                for response, tag in responses:   # type: dict, str
                    try:
                        response['tag'] = tag
                        response['season'] = season
                        war = coc.ClanWar(
                            data=response, client=self.coc_client
                        )
                        if war.preparation_start_time is None:
                            continue
                        custom_id = hashids.encode(
                            int(
                                war.preparation_start_time.time.replace(
                                    tzinfo=pend.UTC
                                ).timestamp()
                            )
                            + int(pend.now(tz=pend.UTC).timestamp())
                            + random.randint(1000000000, 9999999999)
                        )
                        war_unique_id = (
                            '-'.join(sorted([war.clan.tag, war.opponent.tag]))
                            + f'-{int(war.preparation_start_time.time.timestamp())}'
                        )

                        add_war.append(
                            {
                                'war_id': war_unique_id,
                                'custom_id': custom_id,
                                'data': war._raw_data,
                                'type': 'cwl',
                            }
                        )
                    except Exception as e:
                        self.logger.error(
                            f'Error processing war data for tag {tag}: {e}'
                        )

                self.logger.info(
                    f'Working on adding | {time.time() - start_time} sec'
                )
                if add_war:
                    try:
                        await self.db_client.clan_wars.insert_many(
                            documents=add_war, ordered=False
                        )
                        self.logger.info(
                            f'{len(add_war)} Wars Updated/Inserted | {time.time() - start_time} sec'
                        )
                    except Exception as e:
                        self.logger.error(f'Error inserting wars: {e}')
        except Exception as e:
            self.logger.exception(f'Unexpected error in store_cwl_wars: {e}')

    async def store_cwl_groups(self):
        """
        Store Clan War League groups data at scheduled times.
        """
        try:
            season = self.gen_games_season()

            async def fetch_group(
                url, session: aiohttp.ClientSession, headers, tag
            ):
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        return ((await response.json()), tag)
                    return (None, tag)

            pipeline = [{'$match': {}}, {'$group': {'_id': '$tag'}}]
            all_tags = [
                x['_id']
                for x in (
                    await self.db_client.basic_clan.aggregate(
                        pipeline
                    ).to_list(length=None)
                )
            ]

            pipeline = [
                {
                    '$match': {
                        '$and': [
                            {'data.season': season},
                            {'data.state': 'ended'},
                        ]
                    }
                },
                {'$group': {'_id': '$data.clans.tag'}},
            ]
            done_for_this_season = [
                x['_id']
                for x in (
                    await self.db_client.cwl_group.aggregate(pipeline).to_list(
                        length=None
                    )
                )
            ]
            done_for_this_season = set(
                [j for sub in done_for_this_season for j in sub]
            )

            all_tags = [
                tag for tag in all_tags if tag not in done_for_this_season
            ]

            size_break = 50000
            all_tags = [
                all_tags[i : i + size_break]
                for i in range(0, len(all_tags), size_break)
            ]

            was_found_in_a_previous_group = set()
            for tag_group in all_tags:
                tasks = []
                connector = aiohttp.TCPConnector(limit=250, ttl_dns_cache=300)
                timeout = aiohttp.ClientTimeout(total=1800)
                async with aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    json_serialize=ujson.dumps,
                ) as session:
                    for tag in tag_group:
                        if tag in was_found_in_a_previous_group:
                            continue
                        tasks.append(
                            fetch_group(
                                f"https://api.clashofclans.com/v1/clans/{tag.replace('#', '%23')}/currentwar/leaguegroup",
                                session,
                                {
                                    'Authorization': f'Bearer {next(self.coc_client.http.keys)}'
                                },
                                tag,
                            )
                        )
                    responses = await asyncio.gather(
                        *tasks, return_exceptions=True
                    )

                changes = []
                responses = [
                    r
                    for r in responses
                    if isinstance(r, tuple) and r[0] is not None
                ]
                for response, tag in responses:
                    try:
                        season = response.get('season')
                        for clan in response.get('clans', []):
                            was_found_in_a_previous_group.add(clan.get('tag'))
                        tags = sorted(
                            [
                                clan.get('tag').replace('#', '')
                                for clan in response.get('clans', [])
                            ]
                        )
                        cwl_id = f"{season}-{'-'.join(tags)}"
                        changes.append(
                            UpdateOne(
                                {'cwl_id': cwl_id},
                                {'$set': {'data': response}},
                                upsert=True,
                            )
                        )
                    except Exception as e:
                        self.logger.error(
                            f'Error processing cwl_group data for tag {tag}: {e}'
                        )
                if changes:
                    try:
                        await self.db_client.cwl_group.bulk_write(changes)
                        self.logger.info(
                            f'{len(changes)} Changes Updated/Inserted'
                        )
                    except Exception as e:
                        self.logger.error(f'Error writing cwl_group: {e}')
        except Exception as e:
            self.logger.exception(f'Unexpected error in store_cwl_groups: {e}')

    async def update_autocomplete(self):
        """
        Update the autocomplete data for players every 30 minutes.
        """
        try:
            current_time = pend.now(tz=pend.UTC)
            one_hour_ago = current_time.subtract(hours=1).timestamp()
            # Any players that had an update in the last hour
            pipeline = [
                {'$match': {'last_updated': {'$gte': one_hour_ago}}},
                {'$project': {'tag': '$tag'}},
                {'$unset': '_id'},
            ]
            all_player_tags = [
                x['tag']
                for x in (
                    await self.db_client.player_stats.aggregate(
                        pipeline
                    ).to_list(length=None)
                )
            ]

            split_size = 50_000
            split_tags = [
                all_player_tags[i : i + split_size]
                for i in range(0, len(all_player_tags), split_size)
            ]

            for count, group in enumerate(split_tags, 1):
                t = time.time()
                self.logger.info(f'Group {count}/{len(split_tags)}')
                tasks = []
                previous_player_responses = await self.redis.mget(keys=group)
                for response in previous_player_responses:
                    if response is not None:
                        try:
                            response = orjson.loads(
                                snappy.decompress(response)
                            )
                            d = {
                                'name': response.get('name'),
                                'clan': response.get('clan', {}).get(
                                    'tag', 'No Clan'
                                ),
                                'league': response.get('league', {}).get(
                                    'name', 'Unranked'
                                ),
                                'tag': response.get('tag'),
                                'th': response.get('townHallLevel'),
                                'clan_name': response.get('clan', {}).get(
                                    'name', 'No Clan'
                                ),
                                'trophies': response.get('trophies'),
                            }
                            tasks.append(
                                UpdateOne(
                                    {'tag': response.get('tag')},
                                    {'$set': d},
                                    upsert=True,
                                )
                            )
                        except Exception as e:
                            self.logger.error(
                                f'Error processing player data: {e}'
                            )
                self.logger.info(
                    f'Starting bulk write: took {time.time() - t} secs'
                )
                if tasks:
                    try:
                        await self.db_client.player_autocomplete.bulk_write(
                            tasks, ordered=False
                        )
                        self.logger.info(
                            f'Updated autocomplete for {len(tasks)} players'
                        )
                    except Exception as e:
                        self.logger.error(f'Error updating autocomplete: {e}')
        except Exception as e:
            self.logger.exception(
                f'Unexpected error in update_autocomplete: {e}'
            )

    async def update_region_leaderboards(self):
        """
        Update regional leaderboards every 15 minutes.
        """
        try:
            # Reset ranks
            await self.db_client.region_leaderboard.update_many(
                {}, {'$set': {'global_rank': None, 'local_rank': None}}
            )
            lb_changes = []
            tasks = [
                asyncio.create_task(
                    self.coc_client.get_location_players(location_id=location)
                )
                for location in locations
            ]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            for index, response in enumerate(responses):
                if isinstance(response, Exception):
                    self.logger.error(
                        f'Error fetching players for location {locations[index]}: {response}'
                    )
                    continue
                location = locations[index]
                if location != 'global':
                    try:
                        location_obj = await self.coc_client.get_location(
                            location
                        )
                    except Exception as e:
                        self.logger.error(
                            f'Error fetching location details for {location}: {e}'
                        )
                        continue
                for player in response:
                    if not isinstance(player, coc.RankedPlayer):
                        continue
                    if location == 'global':
                        lb_changes.append(
                            UpdateOne(
                                {'tag': player.tag},
                                {'$set': {f'global_rank': player.rank}},
                                upsert=True,
                            )
                        )
                    else:
                        lb_changes.append(
                            UpdateOne(
                                {'tag': player.tag},
                                {
                                    '$set': {
                                        f'local_rank': player.rank,
                                        f'country_name': location_obj.name,
                                        f'country_code': location_obj.country_code,
                                    }
                                },
                                upsert=True,
                            )
                        )

            if lb_changes:
                try:
                    await self.db_client.region_leaderboard.bulk_write(
                        lb_changes
                    )
                    self.logger.info(
                        f'Updated region leaderboards with {len(lb_changes)} changes'
                    )
                except Exception as e:
                    self.logger.error(
                        f'Error updating region leaderboards: {e}'
                    )

            # Reset builder ranks
            await self.db_client.region_leaderboard.update_many(
                {},
                {
                    '$set': {
                        'builder_global_rank': None,
                        'builder_local_rank': None,
                    }
                },
            )
            lb_changes = []
            tasks = [
                asyncio.create_task(
                    self.coc_client.get_location_players_builder_base(
                        location_id=location
                    )
                )
                for location in locations
            ]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            for index, response in enumerate(responses):
                if isinstance(response, Exception):
                    self.logger.error(
                        f'Error fetching builder players for location {locations[index]}: {response}'
                    )
                    continue
                location = locations[index]
                if location != 'global':
                    try:
                        location_obj = await self.coc_client.get_location(
                            location
                        )
                    except Exception as e:
                        self.logger.error(
                            f'Error fetching location details for {location}: {e}'
                        )
                        continue
                for player in response:
                    if not isinstance(player, coc.RankedPlayer):
                        continue
                    if location == 'global':
                        lb_changes.append(
                            UpdateOne(
                                {'tag': player.tag},
                                {
                                    '$set': {
                                        f'builder_global_rank': player.builder_base_rank
                                    }
                                },
                                upsert=True,
                            )
                        )
                    else:
                        lb_changes.append(
                            UpdateOne(
                                {'tag': player.tag},
                                {
                                    '$set': {
                                        f'builder_local_rank': player.builder_base_rank,
                                        f'country_name': location_obj.name,
                                        f'country_code': location_obj.country_code,
                                    }
                                },
                                upsert=True,
                            )
                        )

            if lb_changes:
                try:
                    await self.db_client.region_leaderboard.bulk_write(
                        lb_changes
                    )
                    self.logger.info(
                        f'Updated builder leaderboards with {len(lb_changes)} changes'
                    )
                except Exception as e:
                    self.logger.error(
                        f'Error updating builder leaderboards: {e}'
                    )
        except Exception as e:
            self.logger.exception(
                f'Unexpected error in update_region_leaderboards: {e}'
            )

    async def store_clan_capital(self):
        """
        Store clan capital raid seasons data.
        """
        try:
            pipeline = [{'$match': {}}, {'$group': {'_id': '$tag'}}]
            all_tags = [
                x['_id']
                for x in (
                    await self.db_client.global_clans.aggregate(
                        pipeline
                    ).to_list(length=None)
                )
            ]
            all_tags = [
                all_tags[i : i + self.batch_size]
                for i in range(0, len(all_tags), self.batch_size)
            ]
            now = pend.now()  # Current time in your system's timezone
            start_of_week = now.start_of('week').subtract(days=1)
            end_of_week = start_of_week.add(weeks=1)

            for tag_group in all_tags:
                tasks = []
                for tag in tag_group:
                    tasks.append(
                        self.fetch(
                            url=f"https://api.clashofclans.com/v1/clans/{tag.replace('#', '%23')}/capitalraidseasons?limit=1",
                            tag=tag,
                            json=True,
                        )
                    )
                responses = await asyncio.gather(
                    *tasks, return_exceptions=True
                )

                changes = []
                responses = [
                    r
                    for r in responses
                    if isinstance(r, tuple) and r[0] is not None
                ]
                for response, tag in responses:
                    try:
                        # We shouldn't have completely invalid tags, they all existed at some point
                        if not response['items']:
                            continue
                        date = pend.instance(
                            coc.Timestamp(
                                data=response['items'][0]['endTime']
                            ).time,
                            tz=pend.UTC,
                        )
                        if start_of_week <= date <= end_of_week:
                            changes.append(
                                InsertOne(
                                    {
                                        'clan_tag': tag,
                                        'data': response['items'][0],
                                    }
                                )
                            )
                    except Exception as e:
                        self.logger.error(
                            f'Error processing clan capital data for tag {tag}: {e}'
                        )

                try:
                    self.logger.info(f'{len(changes)} CHANGES')
                    if changes:
                        await self.db_client.raid_weekends.bulk_write(
                            changes, ordered=False
                        )
                except Exception as e:
                    self.logger.error(f'Error writing raid_weekends: {e}')

            # Uncomment if needed
            # await calculate_player_capital_looted_leaderboards(db_client=self.db_client)
            # await calculate_clan_capital_leaderboards(db_client=self.db_client)
            # await calculate_raid_medal_leaderboards(db_client=self.db_client)
        except Exception as e:
            self.logger.exception(
                f'Unexpected error in store_clan_capital: {e}'
            )

    async def find_new_clans(self):
        now = pend.now()
        cutoff_date = now.subtract(days=30)

        pipeline_1 = [
            {'$match': {'endTime': {'$gte': int(cutoff_date.timestamp())}}},
            {'$unwind': '$clans'},
            {
                '$group': {'_id': '$clans'}
            },  # Keep unique clans as separate documents
        ]
        result_1 = await self.db_client.clan_wars.aggregate(
            pipeline_1
        ).to_list(length=None)
        unique_clans = {doc['_id'] for doc in result_1}

        pipeline_2 = [
            {
                '$match': {
                    'data.endTime': {
                        '$gte': cutoff_date.strftime('%Y%m%dT%H%M%S.000Z')
                    }
                }
            },
            {
                '$unwind': {
                    'path': '$data.attackLog',
                    'preserveNullAndEmptyArrays': True,
                }
            },
            {'$group': {'_id': '$data.attackLog.defender.tag'}},
        ]
        attack_tags = await self.db_client.raid_weekends.aggregate(
            pipeline_2
        ).to_list(length=None)
        attack_tags = {doc['_id'] for doc in attack_tags if doc['_id']}

        pipeline_3 = [
            {
                '$match': {
                    'data.endTime': {
                        '$gte': cutoff_date.strftime('%Y%m%dT%H%M%S.000Z')
                    }
                }
            },
            {
                '$unwind': {
                    'path': '$data.defenseLog',
                    'preserveNullAndEmptyArrays': True,
                }
            },
            {'$group': {'_id': '$data.defenseLog.attacker.tag'}},
        ]
        defense_tags = await self.db_client.raid_weekends.aggregate(
            pipeline_3
        ).to_list(length=None)
        defense_tags = {doc['_id'] for doc in defense_tags if doc['_id']}

        unique_tags = attack_tags.union(defense_tags)

        combined_set = unique_clans.union(unique_tags)

        pipeline_4 = [{'$match': {}}, {'$group': {'_id': '$tag'}}]
        existing_clans = [
            x['_id']
            for x in (
                await self.db_client.global_clans.aggregate(
                    pipeline_4
                ).to_list(length=None)
            )
        ]
        existing_clans = set(existing_clans)
        # Find clans in the combined set but not in existing_clans
        new_clans = combined_set - existing_clans
        print(len(new_clans))
        tags_to_add = []
        for clan in new_clans:
            tags_to_add.append(InsertOne({'tag': clan}))
        results = await self.db_client.global_clans.bulk_write(
            tags_to_add, ordered=False
        )
        print(results.bulk_api_result)


if __name__ == '__main__':
    tracker = ScheduledTracking(tracker_type=TrackingType.GLOBAL_SCHEDULED)
    asyncio.run(
        tracker.run(
            tracker_class=ScheduledTracking,
            use_scheduler=True,
            setup_scheduler_method=lambda tracker: tracker.setup_scheduler(),
        )
    )
