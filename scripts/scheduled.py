import asyncio
import random
import time
from collections import defaultdict

import coc
import orjson
import pendulum as pend
import snappy
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from hashids import Hashids
from pymongo import InsertOne, UpdateOne

from utility.constants import locations


from .tracking import Tracking, TrackingType
from utility.time import gen_games_season, gen_raid_date, gen_season_date, season_start_end, CLASH_ISO_FORMAT


class ScheduledTracking(Tracking):
    def __init__(self):
        super().__init__(tracker_type=TrackingType.GLOBAL_SCHEDULED, batch_size=50_000)

    async def store_cwl_wars(self):
        """
        Store Clan War League wars data at scheduled times.
        """
        try:
            hashids = Hashids(min_length=7)
            season = gen_games_season()
            pipeline = [{"$match": {"data.season": season}}, {"$group": {"_id": "$data.rounds.warTags"}}]
            result = await self.async_mongo.cwl_group.aggregate(pipeline)
            result = await result.to_list(length=None)
            done_for_this_season = [x["_id"] for x in result]
            done_for_this_season = [j for sub in done_for_this_season for j in sub]
            all_tags = set([j for sub in done_for_this_season for j in sub])
            self.logger.info(f"{len(all_tags)} war tags total")

            pipeline = [{"$match": {"data.season": season}}, {"$group": {"_id": "$data.tag"}}]
            result = await self.async_mongo.clan_wars.aggregate(pipeline)
            tags_already_found = set([x["_id"] for x in await result.to_list(length=None)])

            self.logger.info(f"{len(tags_already_found)} war tags already found")
            all_tags = list(set([t for t in all_tags if t not in tags_already_found]))

            self.logger.info(f"{len(all_tags)} war tags to find")

            batches = self._split_into_batch(items=all_tags)
            for count, tag_group in enumerate(batches, 1):
                start_time = time.time()
                self.logger.info(f"GROUP {count} | {len(tag_group)} tags")
                tasks = []
                for tag in tag_group:
                    tasks.append(
                        self.fetch(
                            url=f"https://api.clashofclans.com/v1/clanwarleagues/wars/{tag.replace('#', '%23')}",
                            tag=tag,
                            json=True
                        )
                    )
                self.logger.info(f"{len(tasks)} tasks")
                responses = await self._run_tasks(tasks=tasks, return_exceptions=True, wrapped=True)
                add_war = []

                for response in responses:  # type: dict, str
                    if not isinstance(response, tuple):
                        continue
                    response, tag = response
                    try:
                        response["tag"] = tag
                        response["season"] = season
                        war = coc.ClanWar(data=response, client=self.coc_client)
                        if war.preparation_start_time is None:
                            continue

                        if war.state != coc.enums.WarState.war_ended:
                            continue

                        custom_id = hashids.encode(
                            int(war.preparation_start_time.time.replace(tzinfo=pend.UTC).timestamp())
                            + int(pend.now(tz=pend.UTC).timestamp())
                            + random.randint(1000000000, 9999999999)
                        )
                        war_unique_id = (
                            "-".join(sorted([war.clan.tag, war.opponent.tag]))
                            + f"-{int(war.preparation_start_time.time.timestamp())}"
                        )

                        add_war.append(
                            {"war_id": war_unique_id, "custom_id": custom_id, "data": war._raw_data, "type": "cwl"}
                        )
                    except Exception as e:
                        self.logger.error(f"Error processing war data for tag {tag}: {e}")

                self.logger.info(f"Working on adding | {time.time() - start_time} sec")
                if add_war:
                    try:
                        await self.async_mongo.clan_wars.insert_many(add_war, ordered=False)
                    except Exception as e:
                        self.logger.error(str(e))

                    self.logger.info(f"{len(add_war)} Wars inserted {time.time() - start_time:.2f} sec")

        except Exception as e:
            self.logger.exception(f"Unexpected error in store_cwl_wars: {e}")

    async def store_cwl_groups(self):
        """
        Store Clan War League groups data at scheduled times.
        """
        try:
            season = gen_games_season()

            pipeline = [{"$match": {}}, {"$group": {"_id": "$tag"}}]
            pipeline = await self.async_mongo.all_clans.aggregate(pipeline=pipeline)
            pipeline = await pipeline.to_list(length=None)
            all_tags = [x["_id"] for x in pipeline]

            pipeline = [
                {"$match": {"$and": [{"data.season": season}, {"data.state": "ended"}]}},
                {"$group": {"_id": "$data.clans.tag"}},
            ]
            pipeline = await self.async_mongo.cwl_group.aggregate(pipeline=pipeline)
            pipeline = await pipeline.to_list(length=None)
            done_for_this_season = [x["_id"] for x in pipeline]
            done_for_this_season = set([j for sub in done_for_this_season for j in sub])

            all_tags = [tag for tag in all_tags if tag not in done_for_this_season]

            tag_batches = self._split_into_batch(items=all_tags)
            was_found_in_a_previous_group = set()
            for tag_group in tag_batches:
                tasks = []
                for tag in tag_group:
                    if tag in was_found_in_a_previous_group:
                        continue
                    tasks.append(
                        self.fetch(
                            url=f"https://api.clashofclans.com/v1/clans/{tag.replace('#', '%23')}/currentwar/leaguegroup",
                            tag=tag, json=True
                        )
                    )
                responses = await self._run_tasks(tasks=tasks, return_exceptions=True, wrapped=True)

                changes = []
                for response in responses:
                    if not isinstance(response, tuple):
                        continue
                    response, tag = response
                    try:
                        season = response.get("season")
                        for clan in response.get("clans", []):
                            was_found_in_a_previous_group.add(clan.get("tag"))
                        tags = sorted([clan.get("tag").replace("#", "") for clan in response.get("clans", [])])
                        cwl_id = f"{season}-{'-'.join(tags)}"
                        changes.append(UpdateOne({"cwl_id": cwl_id}, {"$set": {"data": response}}, upsert=True))
                    except Exception as e:
                        self.logger.error(f"Error processing cwl_group data for tag {tag}: {e}")
                if changes:
                    try:
                        await self.async_mongo.cwl_group.bulk_write(changes)
                        self.logger.info(f"{len(changes)} Changes Updated/Inserted")
                    except Exception as e:
                        self.logger.error(f"Error writing cwl_group: {e}")

        except Exception as e:
            self.logger.exception(f"Unexpected error in store_cwl_groups: {e}")

    async def store_all_leaderboards(self):
        """
        Store all leaderboards data at scheduled times.
        """
        try:
            for database, function in zip(
                [
                    self.async_mongo.clan_trophies,
                    self.async_mongo.clan_versus_trophies,
                    self.async_mongo.capital,
                    self.async_mongo.player_trophies,
                    self.async_mongo.player_versus_trophies,
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
                    function(location_id=location) for location in locations
                ]
                responses = await asyncio.gather(*tasks, return_exceptions=True)
                store_tasks = []
                for index, response in enumerate(responses):
                    if isinstance(response, Exception):
                        self.logger.error(f"Error fetching data for location {locations[index]}: {response}")
                        continue
                    location = locations[index]
                    store_tasks.append(
                        InsertOne(
                            {
                                "location": location,
                                "date": str(pend.now(tz=pend.UTC).date()),
                                "data": {"items": [x._raw_data for x in response]},
                            }
                        )
                    )

                if store_tasks:
                    try:
                        await database.bulk_write(store_tasks)
                        self.logger.info("Stored leaderboards for locations batch.")
                    except Exception as e:
                        self.logger.error(f"Error storing leaderboards: {e}")
        except Exception as e:
            self.logger.exception(f"Unexpected error in store_all_leaderboards: {e}")

        await self.async_mongo.legend_rankings.aggregate(
            [{"$match" : {}}, {"$out" : {"db" : "ranking_history", "coll" : "legends"}}]
        )

    async def store_legends(self):
        """
        Store legends data at scheduled times.
        """
        seasons = await self.coc_client.get_seasons(league_id=29000022)

        seasons_present = await self.async_mongo.legend_history.distinct("season")
        missing = set(seasons) - set(seasons_present)

        for season_id in missing:
            rankings = await self.coc_client.get_season_rankings(
                league_id=29000022,
                season_id=season_id, limit=25_000
            )
            changes = []
            async for ranking in rankings:
                data = ranking._raw_data | {"season" : season_id}
                changes.append(InsertOne(data))

            if changes:
                await self.async_mongo.legend_history.bulk_write(changes, ordered=False)
                self.logger.info(f"Inserted {len(changes)} legend records for season {season_id}")

    async def _get_cache_tags(self):
        cursor = 0
        keys = []
        while True:
            cursor, batch = await self.aredis_decoded.scan(cursor=cursor, match="player-cache:*", count=25_000)
            keys.extend(batch)
            if cursor == 0:
                break
        return set(keys)

    async def update_autocomplete(self):
        """
        Update the autocomplete data for players every 30 minutes.
        """
        # Any players that had an update in the last hour
        all_player_tags = await self._get_cache_tags()
        player_tag_batches = self._split_into_batch(items=list(all_player_tags))

        for count, group in enumerate(player_tag_batches, 1):
            t = time.time()
            self.logger.info(f"Group {count}/{len(player_tag_batches)}")
            previous_player_responses = self.redis_raw.mget(keys=group)
            pipeline = self.aredis_decoded.pipeline()
            for response, tag in zip(previous_player_responses, group):
                if not response:
                    continue
                try:
                    response = orjson.loads(snappy.decompress(response))
                    d = {
                        "name": response.get("name"),
                        "clan": response.get("clan", {}).get("tag", "No Clan"),
                        "league": response.get("league", {}).get("name", "Unranked"),
                        "tag": response.get("tag"),
                        "th": response.get("townHallLevel"),
                        "clan_name": response.get("clan", {}).get("name", "No Clan"),
                        "trophies": response.get("trophies"),
                    }
                    pipeline.hset(f"search:{tag}", mapping=d)
                    pipeline.expire(f"search:{tag}", 86400)
                except Exception as e:
                    self.logger.error(f"Error processing player data: {e}")

            self.logger.info(f"Starting bulk write: took {time.time() - t} secs")
            try:
                await pipeline.execute()
                self.logger.info(f"Updated autocomplete for {len(group)} players")
            except Exception as e:
                self.logger.error(f"Error updating autocomplete: {e}")

    async def update_region_leaderboards(self):
        """
        Update regional leaderboards every 15 minutes.
        """
        try:
            # Reset ranks
            await self.async_mongo.region_leaderboard.update_many({}, {"$set": {"global_rank": None, "local_rank": None}})
            lb_changes = []
            tasks = [
                asyncio.create_task(self.coc_client.get_location_players(location_id=location))
                for location in locations
            ]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            for index, response in enumerate(responses):
                if isinstance(response, Exception):
                    self.logger.error(f"Error fetching players for location {locations[index]}: {response}")
                    continue
                location = locations[index]
                if location != "global":
                    try:
                        location_obj = await self.coc_client.get_location(location)
                    except Exception as e:
                        self.logger.error(f"Error fetching location details for {location}: {e}")
                        continue
                for player in response:
                    if not isinstance(player, coc.RankedPlayer):
                        continue
                    if location == "global":
                        lb_changes.append(
                            UpdateOne({"tag": player.tag}, {"$set": {"global_rank": player.rank}}, upsert=True)
                        )
                    else:
                        lb_changes.append(
                            UpdateOne(
                                {"tag": player.tag},
                                {
                                    "$set": {
                                        "local_rank": player.rank,
                                        "country_name": location_obj.name,
                                        "country_code": location_obj.country_code,
                                    }
                                },
                                upsert=True,
                            )
                        )

            if lb_changes:
                try:
                    await self.async_mongo.region_leaderboard.bulk_write(lb_changes)
                    self.logger.info(f"Updated region leaderboards with {len(lb_changes)} changes")
                except Exception as e:
                    self.logger.error(f"Error updating region leaderboards: {e}")

            # Reset builder ranks
            await self.async_mongo.region_leaderboard.update_many(
                {}, {"$set": {"builder_global_rank": None, "builder_local_rank": None}}
            )
            lb_changes = []
            tasks = [
                asyncio.create_task(self.coc_client.get_location_players_builder_base(location_id=location))
                for location in locations
            ]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            for index, response in enumerate(responses):
                if isinstance(response, Exception):
                    self.logger.error(f"Error fetching builder players for location {locations[index]}: {response}")
                    continue
                location = locations[index]
                if location != "global":
                    try:
                        location_obj = await self.coc_client.get_location(location)
                    except Exception as e:
                        self.logger.error(f"Error fetching location details for {location}: {e}")
                        continue
                for player in response:
                    if not isinstance(player, coc.RankedPlayer):
                        continue
                    if location == "global":
                        lb_changes.append(
                            UpdateOne(
                                {"tag": player.tag},
                                {"$set": {"builder_global_rank": player.builder_base_rank}},
                                upsert=True,
                            )
                        )
                    else:
                        lb_changes.append(
                            UpdateOne(
                                {"tag": player.tag},
                                {
                                    "$set": {
                                        "builder_local_rank": player.builder_base_rank,
                                        "country_name": location_obj.name,
                                        "country_code": location_obj.country_code,
                                    }
                                },
                                upsert=True,
                            )
                        )

            if lb_changes:
                try:
                    await self.async_mongo.region_leaderboard.bulk_write(lb_changes)
                    self.logger.info(f"Updated builder leaderboards with {len(lb_changes)} changes")
                except Exception as e:
                    self.logger.error(f"Error updating builder leaderboards: {e}")
        except Exception as e:
            self.logger.exception(f"Unexpected error in update_region_leaderboards: {e}")

    async def store_clan_capital(self):
        """
        Store clan capital raid seasons data.
        """
        try:
            bot_clans = await self.async_mongo.clans_db.distinct("tag")
            bookmarked_clans = await self.async_mongo.user_settings.distinct("search.clan.bookmarked")

            all_clans = list(set(bot_clans + bookmarked_clans))

            now = pend.now()  # Current time in your system's timezone
            start_of_week = now.start_of("week").subtract(days=1)
            end_of_week = start_of_week.add(weeks=1)

            tasks = []
            for tag in all_clans:
                tasks.append(
                    self.fetch(
                        url=f"https://api.clashofclans.com/v1/clans/{tag.replace('#', '%23')}/capitalraidseasons?limit=1",
                        tag=tag,
                        json=True,
                    )
                )
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            changes = []
            responses = [r for r in responses if isinstance(r, tuple) and r[0] is not None]
            for response, tag in responses:
                try:
                    # We shouldn't have completely invalid tags, they all existed at some point
                    if not response["items"]:
                        continue
                    date = pend.instance(coc.Timestamp(data=response["items"][0]["endTime"]).time, tz=pend.UTC)
                    if start_of_week <= date <= end_of_week:
                        changes.append(InsertOne({"clan_tag": tag, "data": response["items"][0]}))
                except Exception as e:
                    self.logger.error(f"Error processing clan capital data for tag {tag}: {e}")

            try:
                self.logger.info(f"{len(changes)} CHANGES")
                if changes:
                    await self.async_mongo.raid_weekends.bulk_write(changes, ordered=False)
            except Exception as e:
                self.logger.error(f"Error writing raid_weekends: {e}")

            # Uncomment if needed
            # await calculate_player_capital_looted_leaderboards(db_client=self.db_client)
            # await calculate_clan_capital_leaderboards(db_client=self.db_client)
            # await calculate_raid_medal_leaderboards(db_client=self.db_client)
        except Exception as e:
            self.logger.exception(f"Unexpected error in store_clan_capital: {e}")

    async def find_new_clans(self):
        now = pend.now()
        cutoff_date = now.subtract(days=30)

        pipeline_1 = [
            {"$match": {"endTime": {"$gte": int(cutoff_date.timestamp())}}},
            {"$unwind": "$clans"},
            {"$group": {"_id": "$clans"}},  # Keep unique clans as separate documents
        ]
        result_1 = await self.async_mongo.clan_wars.aggregate(pipeline_1)
        result_1 = await result_1.to_list(length=None)
        unique_clans = {doc["_id"] for doc in result_1}

        pipeline_2 = [
            {"$match": {"data.endTime": {"$gte": cutoff_date.strftime("%Y%m%dT%H%M%S.000Z")}}},
            {"$unwind": {"path": "$data.attackLog", "preserveNullAndEmptyArrays": True}},
            {"$group": {"_id": "$data.attackLog.defender.tag"}},
        ]
        attack_tags = await self.async_mongo.raid_weekends.aggregate(pipeline_2)
        attack_tags = await attack_tags.to_list(length=None)
        attack_tags = {doc["_id"] for doc in attack_tags if doc["_id"]}

        pipeline_3 = [
            {"$match": {"data.endTime": {"$gte": cutoff_date.strftime("%Y%m%dT%H%M%S.000Z")}}},
            {"$unwind": {"path": "$data.defenseLog", "preserveNullAndEmptyArrays": True}},
            {"$group": {"_id": "$data.defenseLog.attacker.tag"}},
        ]
        defense_tags = await self.async_mongo.raid_weekends.aggregate(pipeline_3)
        defense_tags = await defense_tags.to_list(length=None)
        defense_tags = {doc["_id"] for doc in defense_tags if doc["_id"]}

        unique_tags = attack_tags.union(defense_tags)

        combined_set = unique_clans.union(unique_tags)

        pipeline_4 = [{"$match": {}}, {"$group": {"_id": "$tag"}}]
        existing_clans = [
            x["_id"] for x in (await (await self.async_mongo.global_clans.aggregate(pipeline_4)).to_list(length=None))
        ]
        existing_clans = set(existing_clans)
        # Find clans in the combined set but not in existing_clans
        new_clans = combined_set - existing_clans
        print(len(new_clans))

        tags_to_add = []
        for clan in new_clans:
            tags_to_add.append(InsertOne({"tag": clan, "active": True}))
        results = await self.async_mongo.all_clans.bulk_write(tags_to_add, ordered=False)
        print(results.bulk_api_result)

    async def set_active_clans(self):
        ninety_days = pend.now(tz=pend.UTC).subtract(days=90).int_timestamp

        pipeline = [
            {"$match": {"$and": [{"endTime": {"$gte": ninety_days}}, {"type": {"$ne": "cwl"}}]}},
            {"$unwind": "$clans"},
            {"$group": {"_id": "$clans"}},
        ]
        active_clans = [
            x["_id"] for x in
            await (await self.async_mongo.clan_wars.aggregate(pipeline)).to_list(length=None)
        ]

        self.logger.info(f"ACTIVE CLANS: {len(active_clans)}")
        await self.async_mongo.all_clans.update_many(
            {"$or": [
                {"data.members": {"$lt": 5}},
                {"data.clanLevel": {"$lt": 3}},
                {"data.capitalLeague.name": None}
            ]},
            {"$set": {"active": False}},
        )

        await self.async_mongo.all_clans.update_many(
            {"$nor": [
                {"data.members": {"$lt": 5}},
                {"data.clanLevel": {"$lt": 3}},
                {"data.capitalLeague.name": None}
            ]},
            {"$set": {"active": True}},
        )
        batches = self._split_into_batch(items=active_clans)
        for clans in batches:
            await self.async_mongo.all_clans.update_many(
                {"tag": {"$in": clans}},
                {"$set": {"active": True}},
            )

    async def remove_dead_clans(self):
        thirty_five_days = pend.now(tz=pend.UTC).subtract(days=35).int_timestamp

        pipeline = [
            {"$match": {"$and": [{"endTime": {"$gte": thirty_five_days}}]}},
            {"$unwind": "$clans"},
            {"$group": {"_id": "$clans"}},
        ]
        active_clans = [
            x["_id"] for x in
            await (await self.async_mongo.clan_wars.aggregate(pipeline)).to_list(length=None)
        ]

        active_clans = set(active_clans)
        self.logger.info(f"ACTIVE CLANS: {len(active_clans)}")

        pipeline = [
            {"$match": {"$or": [
                {"members": {"$lt": 5}},
                {"level": {"$lt": 3}},
                {"capitalLeague": "Unranked"}
            ]}},
            {"$group": {"_id": "$tag"}},
        ]
        pipeline = await self.async_mongo.global_clans.aggregate(pipeline)
        dead_clans = await pipeline.to_list(length=None)
        dead_clans = [x["_id"] for x in dead_clans]
        clans_to_delete = []
        for clan in dead_clans:
            if clan not in active_clans:
                clans_to_delete.append(clan)

        batches = await self._split_into_batch(items=clans_to_delete)
        for batch in batches:
            await self.async_mongo.all_clans.update_many(
                {"data.tag" : {"$in": batch}},
                {"$unset": {"data": ""}})
        self.logger.info(f"DELETING {len(clans_to_delete)} DEAD CLANS")

    async def test_name_search(self):
        import redis
        import snappy

        config = self.config

        cache = redis.Redis(
            host=config.redis_ip,
            port=6379,
            db=0,
            password=config.redis_pw,
            decode_responses=False,
            max_connections=50,
            health_check_interval=10,
            socket_connect_timeout=5,
            socket_keepalive=True,
        )

        new_cache = redis.Redis(
            host=config.redis_ip,
            port=6379,
            db=1,
            password=config.redis_pw,
            decode_responses=False,
            max_connections=50,
            health_check_interval=10,
            socket_connect_timeout=5,
            socket_keepalive=True,
        )

        def process_batch(keys):
            pipe = cache.pipeline()
            for key in keys:
                pipe.get(key)
            raw_values = pipe.execute()
            nonlocal new_cache
            pipe_dest = new_cache.pipeline()
            for key, raw in zip(keys, raw_values):
                if not raw:
                    continue
                try:
                    data = orjson.loads(snappy.decompress(raw))
                    entry = {
                        "name": data.get("name", ""),
                        "tag": data.get("tag", ""),
                        "clan_name": data.get("clan", {}).get("name", ""),
                        "clan_tag": data.get("clan", {}).get("tag", ""),
                        "league": data.get("league", {}).get("name", ""),
                        "townhall": str(data.get("townHallLevel", "")),
                        "raw_data": raw,
                    }
                    dest_key = f"profile:{entry['tag']}"
                    pipe_dest.hset(dest_key, mapping=entry)
                except Exception as e:
                    print(f"Failed on key {key}: {e}")
            pipe_dest.execute()

        batch = []
        for key in cache.scan_iter(count=1000):  # `count` is a hint, not strict
            batch.append(key)
            if len(batch) >= 1000:
                process_batch(batch)
                batch = []

        # process any leftovers
        if batch:
            process_batch(batch)

    async def better_clan_tracking(self):
        self.mongo.global_clans.update_many(
            {"$or": [{"members": {"$lt": 5}}, {"level": {"$lt": 3}}, {"capitalLeague": "Unranked"}]},
            {"$set": {"active": False}},
        )

        self.mongo.global_clans.update_many(
            {"$nor": [{"members": {"$lt": 5}}, {"level": {"$lt": 3}}, {"capitalLeague": "Unranked"}]},
            {"$set": {"active": True}},
        )

    async def store_league_changes(self, league_type: str):

        if league_type == "capitalLeague":
            timestamp = gen_raid_date()
            spot = "clanCapital"
        elif league_type == "warLeague":
            timestamp = gen_games_season()
            spot = "clanWarLeague"

        updates = []

        async for clan in self.async_mongo.all_clans.find({}, {"_id": 0, "tag": 1, f"data.{league_type}": 1, "data.clanCapitalPoints" : 1}):
            league = clan.get("data").get(league_type).get("name", "Unranked")
            history = {
                "league" : league
            }
            if league_type == "capitalLeague":
                history["trophies"] = clan.get("data").get("clanCapitalPoints", 0)

            updates.append(
                UpdateOne(
                    {"tag" : clan.get("tag")},
                    {"$set" : {f"changes.{spot}.{timestamp}" : history}},

                ))

            if len(updates) >= 25_000:
                await self.async_mongo.clan_change_history.bulk_write(updates, ordered=False)
                updates.clear()

        if updates:
            await self.async_mongo.clan_change_history.bulk_write(updates, ordered=False)

    async def create_stat_leaderboards(self, permanent: bool):
        clan_to_servers = defaultdict(list)
        default_stats_data = [
            "donated",
            "received",
            "activity",
            "dark_elixir_looted",
            "gold_looted",
            "elixir_looted",
            "capital_gold_dono",
            "obstacles_removed",
            "attack_wins",
            "boosted_super_troops",
            "walls_destroyed"
        ]
        server_clans = set()
        async for clan in self.async_mongo.clans_db.find({}, {"_id": 0, "tag": 1, "server": 1}):
            tag = clan.get("tag")
            server_clans.add(tag)
            clan_to_servers[tag].append(clan.get("server"))

        season = gen_season_date()

        for field in default_stats_data:
            if field in ["donated", "received"]:
                player_pipeline = [
                    {"$match": {"season": season}},
                    {"$sort": {field: -1}},
                    {"$limit": 1000},
                    {"$group": {
                        "_id": "$tag",
                        "value": {"$sum": f"${field}"},
                    }},
                    {"$sort": {"value": -1}},
                ]
            else:
                player_pipeline = [
                    {"$match": {"season": season, "activity" : {"$ne": None}}},
                    {"$group": {
                        "_id": "$tag",
                        "value": {"$sum": f"${field}"},
                    }},
                    {"$sort": {"value": -1}},
                    {"$limit": 1000}
                ]
            field_leaderboard = await self.async_mongo.new_player_stats.aggregate(pipeline=player_pipeline)
            field_leaderboard = await field_leaderboard.to_list(length=None)

            field_leaderboard = [f for f in field_leaderboard if f.get("value") != 0]
            if not field_leaderboard:
                continue

            for count, data in enumerate(field_leaderboard, 1):
                data["type"] = field
                data["season"] = season
                data["tag"] = data["_id"]
                data["rank"] = count
                del data["_id"]

            if permanent:
                await self.async_mongo.ranking_history.get_collection("player_leaderboard").insert_many(field_leaderboard)
            else:
                await self.async_mongo.leaderboards.get_collection("player").delete_many({"type": field})
                await self.async_mongo.leaderboards.get_collection("player").insert_many(field_leaderboard)

        for field in default_stats_data:
            if field in ["donated", "received"]:
                clan_pipeline = [
                    {"$match": {"season": season}},
                    {"$group": {
                        "_id": "$clan_tag",
                        "value": {"$sum": f"${field}"},
                    }},
                    {"$sort": {"value": -1}},
                    {"$limit": 1000}
                ]
            else:
                clan_pipeline = [
                    {"$match": {"season": season, "activity": {"$ne": None}}},
                    {"$group": {
                        "_id": "$clan_tag",
                        "value": {"$sum": f"${field}"},
                    }},
                    {"$sort": {"value": -1}},
                    {"$limit": 1000}
                ]
            field_leaderboard = await self.async_mongo.new_player_stats.aggregate(pipeline=clan_pipeline)
            field_leaderboard = await field_leaderboard.to_list(length=None)

            field_leaderboard = [f for f in field_leaderboard if f.get("value") != 0]

            if not field_leaderboard:
                continue

            for count, data in enumerate(field_leaderboard, 1):
                data["type"] = field
                data["season"] = season
                data["tag"] = data["_id"]
                data["rank"] = count
                del data["_id"]

            if permanent:
                await self.async_mongo.ranking_history.get_collection("clan_leaderboard").insert_many(field_leaderboard)
            else:
                await self.async_mongo.leaderboards.get_collection("clan").delete_many({"type": field})
                await self.async_mongo.leaderboards.get_collection("clan").insert_many(field_leaderboard)

        for field in default_stats_data:
            clan_pipeline = [
                {"$match": {"clan_tag": {"$in": list(server_clans)}}},
                {"$match": {"season": season}},
                {"$group": {
                    "_id": "$clan_tag",
                    "value": {"$sum": f"${field}"},
                }},
                {"$sort": {"value": -1}},
            ]
            field_leaderboard = await self.async_mongo.new_player_stats.aggregate(pipeline=clan_pipeline)
            field_leaderboard = await field_leaderboard.to_list(length=None)
            field_leaderboard = [f for f in field_leaderboard if f.get("value") != 0]

            if not field_leaderboard:
                continue

            leaderboard_mapping = {d.get("_id") : d.get("value") for d in field_leaderboard}
            holder_lb = {}
            for clan, servers in clan_to_servers.items():
                for server in servers:
                    if server not in holder_lb:
                        holder_lb[server] = 0
                    holder_lb[server] += leaderboard_mapping.get(clan, 0)

            store_lb = []
            holder_lb = dict(sorted(holder_lb.items(), key=lambda item: item[1], reverse=True))
            for count, (server, value) in enumerate(holder_lb.items(), 1):
                if count > 1000:
                    continue
                store_lb.append({
                    "server": server,
                    "type": field,
                    "value": value,
                    "season": season,
                    "rank": count
                })

            if permanent:
                await self.async_mongo.ranking_history.get_collection("server_leaderboard").insert_many(store_lb)
            else:
                await self.async_mongo.leaderboards.get_collection("server").delete_many({"type": field})
                await self.async_mongo.leaderboards.get_collection("server").insert_many(store_lb)


    async def build_legend_rankings(self):
        ranking_pipeline = [
            {"$match" : {}},
            {"$project" : {"data.memberList" : 1, "_id" : 0}},
            {"$unwind": "$data.memberList"},
            {"$match": {"data.memberList.league.name": "Legend League"}},
            {
                "$project": {
                    "name": "$data.memberList.name",
                    "tag": "$data.memberList.tag",
                    "trophies": "$data.memberList.trophies",
                    "townhall": "$data.memberList.townHallLevel",
                    "sort_field": {"trophies": "$data.memberList.trophies", "tag": "$data.memberList.tag"},
                }
            },
            {"$unset": ["_id"]},
            #window‐function to compute rank over our sort_field
            {"$setWindowFields": {"sortBy": {"sort_field": -1}, "output": {"rank": {"$rank": {}}}}},
            # clean up the temporary sort key
            {"$unset": ["sort_field"]},
            {"$out": {"db": "leaderboards", "coll": "legends"}},
        ]
        cursor = await self.async_mongo.all_clans.aggregate(ranking_pipeline, allowDiskUse=True)
        await cursor.to_list(length=None)


    async def build_cwl_rankings(self):
        self.logger.info("Building CWL Rankings")
        season = gen_games_season()

        pipeline = [
            {"$match": {"data.season": season}},
            {"$group": {"_id": "$cwl_id"}},
        ]
        result = await self.async_mongo.cwl_group.aggregate(pipeline)
        result = await result.to_list(length=None)
        all_cwl_ids = [doc["_id"] for doc in result]

        batched_cwl_ids = self._split_into_batch(items=all_cwl_ids, batch_size=6_000)

        self.logger.info(f"BATCHED CWL IDS: {len(batched_cwl_ids)}")
        for cwl_id_batch in batched_cwl_ids:
            cwl_rank_data = []
            pipeline = [
                {"$match": {"cwl_id": {"$in": cwl_id_batch}}},
                {"$unwind": "$data.rounds"},
                {"$unwind": "$data.rounds.warTags"},
                {
                    "$group": {
                        "_id": "$cwl_id",
                        "warTags": {"$addToSet": "$data.rounds.warTags"}
                    }
                },
                {"$project": {"_id": 0, "cwl_id": "$_id", "warTags": 1}}
            ]
            result = await self.async_mongo.cwl_group.aggregate(pipeline)
            result = await result.to_list(length=None)
            self.logger.info(f"GRABBED CWL ID BATCH")

            war_to_cwl_id = {}
            all_war_tags = []
            for doc in result:
                for tag in doc["warTags"]:
                    if tag == "#0":
                        continue
                    war_to_cwl_id[tag] = doc["cwl_id"]
                    all_war_tags.append(tag)

            self.logger.info(f"EXTRACTED {len(all_war_tags)} WAR TAGS")

            pipeline = [
                {"$match": {"data.tag": {"$in": all_war_tags}}},

                # 1) Compute destruction sums for both sides
                {
                    "$set": {
                        "data.clan.totalDestructionFromAttacks": {
                            "$sum": {
                                "$map": {
                                    "input": {"$ifNull": ["$data.clan.members", []]},
                                    "as": "m",
                                    "in": {
                                        "$sum": {
                                            "$map": {
                                                "input": {"$ifNull": ["$$m.attacks", []]},
                                                "as": "a",
                                                "in": {"$ifNull": ["$$a.destructionPercentage", 0]}
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "data.opponent.totalDestructionFromAttacks": {
                            "$sum": {
                                "$map": {
                                    "input": {"$ifNull": ["$data.opponent.members", []]},
                                    "as": "m",
                                    "in": {
                                        "$sum": {
                                            "$map": {
                                                "input": {"$ifNull": ["$$m.attacks", []]},
                                                "as": "a",
                                                "in": {"$ifNull": ["$$a.destructionPercentage", 0]}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },

                # 2) Decide winner
                {
                    "$set": {
                        "winnerSide": {
                            "$switch": {
                                "branches": [
                                    {"case": {"$gt": ["$data.clan.stars", "$data.opponent.stars"]}, "then": "clan"},
                                    {"case": {"$lt": ["$data.clan.stars", "$data.opponent.stars"]}, "then": "opponent"},
                                    {"case": {"$gt": ["$data.clan.destructionPercentage",
                                                      "$data.opponent.destructionPercentage"]}, "then": "clan"},
                                    {"case": {"$lt": ["$data.clan.destructionPercentage",
                                                      "$data.opponent.destructionPercentage"]}, "then": "opponent"}
                                ],
                                "default": "tie"
                            }
                        }
                    }
                },

                # 3) Flatten into two rows (clan & opponent)
                {
                    "$project": {
                        "sides": [
                            {
                                "tag": "$data.clan.tag",
                                "name": "$data.clan.name",
                                "badge": "$data.clan.badgeUrls.large",
                                "attacks": "$data.clan.attacks",
                                "raw_stars": "$data.clan.stars",
                                "stars_with_bonus": {
                                    "$add": ["$data.clan.stars", {"$cond": [{"$eq": ["$winnerSide", "clan"]}, 10, 0]}]
                                },
                                "win": {"$cond": [{"$eq": ["$winnerSide", "clan"]}, 1, 0]},
                                "totalDestructionFromAttacks": "$data.clan.totalDestructionFromAttacks",
                                "war_tag": "$data.tag"
                            },
                            {
                                "tag": "$data.opponent.tag",
                                "name": "$data.opponent.name",
                                "badge": "$data.opponent.badgeUrls.large",
                                "attacks": "$data.opponent.attacks",
                                "raw_stars": "$data.opponent.stars",
                                "stars_with_bonus": {
                                    "$add": ["$data.opponent.stars",
                                             {"$cond": [{"$eq": ["$winnerSide", "opponent"]}, 10, 0]}]
                                },
                                "win": {"$cond": [{"$eq": ["$winnerSide", "opponent"]}, 1, 0]},
                                "totalDestructionFromAttacks": "$data.opponent.totalDestructionFromAttacks",
                                "war_tag": "$data.tag"
                            }
                        ]
                    }
                },
                {"$unwind": "$sides"},
                {"$replaceRoot": {"newRoot": "$sides"}},

                # 4) Group by clan tag
                {
                    "$group": {
                        "_id": "$tag",
                        "name": {"$last": "$name"},
                        "badge": {"$last": "$badge"},
                        "attacks": {"$sum": "$attacks"},
                        "raw_stars": {"$sum": "$raw_stars"},
                        "stars": {"$sum": "$stars_with_bonus"},
                        "wins": {"$sum": "$win"},
                        "totalDestruction": {"$sum": "$totalDestructionFromAttacks"},
                        "wars": {"$addToSet": "$war_tag"},
                        "war_count": {"$sum": 1}
                    }
                },

                # 5) Final shape
                {
                    "$project": {
                        "_id": 0,
                        "tag": "$_id",
                        "name": 1,
                        "badge": 1,
                        "attacks": 1,
                        "raw_stars": 1,
                        "stars": 1,
                        "totalDestruction": 1,
                        "wars": 1,
                        "wins": 1,
                        "war_count": 1,
                    }
                }
            ]
            result = await self.async_mongo.clan_wars.aggregate(pipeline, allowDiskUse=True)
            self.logger.info(f"GRABBED CWL DATA")
            wars = await result.to_list(length=None)

            self.logger.info(f"PULLED {len(wars)} WARS")

            clan_to_cwl_id = {}
            for war in wars:
                clan_tag = war["tag"]
                war_tag = war["wars"][0]
                cwl_id = war_to_cwl_id.get(war_tag)
                clan_to_cwl_id[clan_tag] = cwl_id
                war["season"] = season
                war["cwl_id"] = cwl_id

            self.logger.info(f"MAPPED WARS")

            previous_season = pend.now(tz=pend.UTC).subtract(months=1).format("YYYY-MM")
            cwl_id_to_league = {}
            cwl_rank_history = self.async_mongo.league_history.find({"tag" : {"$in" : list(clan_to_cwl_id.keys())}},
                                                                   {"_id": 0, "tag": 1,
                                                                    f"changes.clanWarLeague.{previous_season}.league": 1,
                                                                    })
            cwl_rank_history = await cwl_rank_history.to_list(length=None)
            for clan in cwl_rank_history:
                league = clan.get("changes").get("clanWarLeague", {}).get(previous_season, {}).get("league")
                if not league:
                    continue
                cwl_id_to_league[clan_to_cwl_id[clan.get("tag")]] = league

            self.logger.info(f"PULLED CWL LEAGUES")

            for war in wars:
                cwl_id = war.get("cwl_id")
                war["league"] = cwl_id_to_league.get(cwl_id)


            for war in wars:
                cwl_rank_data.append(UpdateOne({"tag" : war["tag"], "season": season}, {"$set" : war}, upsert=True))

            self.logger.info(f"STARTED STORING")

            if cwl_rank_data:
                await self.async_mongo.leaderboards.get_collection("cwl").bulk_write(cwl_rank_data)
            self.logger.info(f"FINISHED STORING")

        pipeline = [
            {
                '$set': {
                    'sort_field': {
                        's': '$stars',
                        'd': '$destructionPercentage'
                    }
                }
            }, {
            '$setWindowFields': {
                'partitionBy': {
                    'season': '$season',
                    'league': '$league',
                    'teamSize': '$teamSize'
                },
                'sortBy': {
                    'sort_field': -1
                },
                'output': {
                    'rank': {
                        '$rank': {}
                    }
                }
            }
        }, {
            '$unset': 'sort_field'
        }, {
            '$merge': {
                'into': 'cwl',
                'on': '_id',
                'whenMatched': 'merge',
                'whenNotMatched': 'discard'
            }
        }
        ]
        await self.async_mongo.leaderboards.get_collection("cwl").aggregate(pipeline)
        self.logger.info("Finished CWL Rankings")


    async def build_hitrate(self):
        todays_date = "2025-08-12"
        now = pend.parse(todays_date)
        for day in range(1, 91):
            war_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

            player_war_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
            day = now.subtract(days=day).replace(hour=0, minute=0, second=0, microsecond=0)
            season = day.format("YYYY-MM")
            day_start = day.format(CLASH_ISO_FORMAT)
            day_end = day.replace(hour=23, minute=59, second=59).format(CLASH_ISO_FORMAT)

            count = 1
            async for war in self.async_mongo.clan_wars.find(
                    {"data.endTime": {"$gte": day_start, "$lte": day_end}},
                    {"_id": 0, "data": 1},
            ):
                war = war.get("data")
                count += 1
                if count % 10_000 == 0:
                    print(f"Processed {count} wars")
                if not war:
                    continue
                war = coc.ClanWar(data=war, client=None)
                if war.type == "friendly":
                    continue
                for attack in war.attacks:
                    versus = f"{attack.attacker.town_hall}v{attack.defender.town_hall}"
                    if attack.attacker.town_hall == attack.defender.town_hall:
                        player_war_stats[attack.attacker_tag][str(attack.attacker.town_hall)]["attacks"] += 1
                        player_war_stats[attack.attacker_tag][str(attack.attacker.town_hall)][f"{attack.stars}_star_attack"] += 1
                    if attack.is_fresh_attack:
                        war_stats[versus]["fresh"]["attacks"] += 1
                        war_stats[versus]["fresh"][f"{attack.stars}_star_attack"] += 1
                    else:
                        war_stats[versus]["non-fresh"]["attacks"] += 1
                        war_stats[versus]["non-fresh"][f"{attack.stars}_star_attack"] += 1

            def defaultdict_to_dict(obj):
                if isinstance(obj, defaultdict):
                    obj = {k: defaultdict_to_dict(v) for k, v in obj.items()}
                elif isinstance(obj, dict):
                    obj = {k: defaultdict_to_dict(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    obj = [defaultdict_to_dict(v) for v in obj]
                return obj

            store_war_data = []
            war_stats = defaultdict_to_dict(war_stats)
            for townhall_versus, freshness_data in war_stats.items():
                townhall, opponent_townhall = townhall_versus.split("v")
                for fresh_type, war_data in freshness_data.items():
                    extended_data = {
                        "townhall": int(townhall),
                        "opponent_townhall": int(opponent_townhall),
                        "fresh" : fresh_type == "fresh",
                        "day": day,
                    }
                    war_data.update(extended_data)
                    store_war_data.append(InsertOne(war_data))
            await self.async_mongo.leaderboards.get_collection("global_hitrates").bulk_write(store_war_data)

            store_player_hitrates = []
            player_war_stats = defaultdict_to_dict(player_war_stats)
            for player_tag, townhall_data in player_war_stats.items():
                for townhall, war_data in townhall_data.items():
                    store_player_hitrates.append(UpdateOne(
                        {"season": season, "player_tag": player_tag, "townhall": int(townhall)},
                        {"$inc": war_data}, upsert=True
                    ))
            await self.async_mongo.leaderboards.get_collection("player_hitrates").bulk_write(store_player_hitrates)


    async def add_permanent_schedules(self):
        self.scheduler.add_job(
            self.store_all_leaderboards,
            CronTrigger(hour=4, minute=56),
            name="Store All Leaderboards",
            misfire_grace_time=300,
        )
        self.scheduler.add_job(
            self.store_legends,
            CronTrigger(day="*", hour=5, minute=56),
            name="Store Legends",
            misfire_grace_time=300
        )
        self.scheduler.add_job(
            self.update_autocomplete,
            IntervalTrigger(minutes=15),
            name="Update Autocomplete",
            misfire_grace_time=300
        )
        self.scheduler.add_job(
            self.update_region_leaderboards,
            IntervalTrigger(minutes=15),
            name="Update Region Leaderboards",
            misfire_grace_time=300,
        )
        self.scheduler.add_job(
            self.store_clan_capital,
            CronTrigger(day_of_week="mon", hour=10),
            name="Store Clan Capital",
            misfire_grace_time=300,
        )

        self.scheduler.add_job(
            self.store_league_changes,
            CronTrigger(day_of_week="mon", hour=12),
            args=["capitalLeague"],
            name="Store Capital League Changes",
            misfire_grace_time=300,
        )

        self.scheduler.add_job(
            self.store_league_changes,
            CronTrigger(day="13", hour="2"),
            args=["warLeague"],
            name="Store War League Changes",
            misfire_grace_time=300,
        )

        self.scheduler.add_job(
            self.build_legend_rankings,
            IntervalTrigger(minutes=20),
            name="Build Global Legend Rankings",
            misfire_grace_time=300,
        )

        self.scheduler.add_job(
            self.create_stat_leaderboards,
            IntervalTrigger(minutes=60),
            args=[False],
            name="Build Stat Leaderboards",
            misfire_grace_time=300,
        )

        self.scheduler.add_job(
            self.create_stat_leaderboards,
            CronTrigger(day="last mon", hour=4, minute=55),
            args=[True],
            name="Build Global Legend Rankings",
            misfire_grace_time=300,
        )

        self.scheduler.add_job(
            self.build_cwl_rankings,
            CronTrigger(day='2-12', hour="*/4", minute=15),
            name="Build CWL Rankings",
            misfire_grace_time=300,
            max_instances=1
        )

        self.scheduler.add_job(
            self.store_cwl_wars,
            CronTrigger(day='2-13', hour="*", minute=5),
            name="Store CWL Wars",
            misfire_grace_time=300,
            max_instances=1
        )
        self.scheduler.add_job(
            self.store_cwl_groups,
            CronTrigger(day='1-12', hour='*', minute=35),
            name="Store CWL Groups",
            misfire_grace_time=300,
        )
        self.scheduler.add_job(
            self.build_hitrate,
            CronTrigger(day='*', hour='14'),
            name="Create Seasonal Hitrate Stats",
            misfire_grace_time=300,
        )


    async def run(self):
        try:
            await self.initialize()
            await self.add_permanent_schedules()
            self.scheduler.start()

            self.logger.info("Scheduler started. Running scheduled jobs...")
            while True:
                await asyncio.sleep(3600)  # Sleep for an hour, adjust as needed
        except (KeyboardInterrupt, SystemExit):
            self.logger.info("Shutting down scheduler...")
            self.scheduler.shutdown()

