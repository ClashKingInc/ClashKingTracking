import asyncio
import random
import time

import aiohttp
import coc
import orjson
import pendulum as pend
import snappy
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from hashids import Hashids
from pymongo import InsertOne, UpdateOne

from utility.constants import locations

import math

from tracking import Tracking, TrackingType
from utility.time import gen_games_season


class ScheduledTracking(Tracking):
    def __init__(self):
        super().__init__(tracker_type=TrackingType.GLOBAL_SCHEDULED, batch_size=25_000)

    async def store_cwl_wars(self):
        """
        Store Clan War League wars data at scheduled times.
        """
        try:
            hashids = Hashids(min_length=7)
            season = gen_games_season()
            pipeline = [{"$match": {"data.season": season}}, {"$group": {"_id": "$data.rounds.warTags"}}]
            result = await self.async_mongo.cwl_group.aggregate(pipeline).to_list(length=None)
            done_for_this_season = [x["_id"] for x in result]
            done_for_this_season = [j for sub in done_for_this_season for j in sub]
            all_tags = set([j for sub in done_for_this_season for j in sub])
            self.logger.info(f"{len(all_tags)} war tags total")

            pipeline = [{"$match": {"data.season": season}}, {"$group": {"_id": "$data.tag"}}]
            tags_already_found = set(
                [x["_id"] for x in (await self.async_mongo.clan_wars.aggregate(pipeline).to_list(length=None))]
            )
            self.logger.info(f"{len(tags_already_found)} war tags already found")
            all_tags = [t for t in all_tags if t not in tags_already_found]

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
                        # 1) split into 10 batches
                        n_batches = 10
                        batch_size = math.ceil(len(add_war) / n_batches)
                        batches = [add_war[i * batch_size : (i + 1) * batch_size] for i in range(n_batches)]

                        # 2) schedule all the insert_many calls
                        tasks = [
                            self.async_mongo.clan_wars.insert_many(batch, ordered=False) for batch in batches if batch
                        ]

                        # 3) run them in parallel, catching exceptions per‐batch
                        results = await asyncio.gather(*tasks, return_exceptions=True)

                        # 4) tally inserted vs. any errors
                        total_inserted = sum(len(res.inserted_ids) for res in results if not isinstance(res, Exception))
                        errors = [e for e in results if isinstance(e, Exception)]

                        for e in errors:
                            self.logger.error(f"Error inserting wars batch: {e}")

                        self.logger.info(
                            f"{total_inserted}/{len(add_war)} Wars inserted "
                            f"in {len(tasks)} batches | {time.time() - start_time:.2f} sec"
                        )

                    except Exception as e:
                        self.logger.error(f"Unexpected batch‐insert error: {e}")
        except Exception as e:
            self.logger.exception(f"Unexpected error in store_cwl_wars: {e}")

    async def store_cwl_groups(self):
        """
        Store Clan War League groups data at scheduled times.
        """
        try:
            season = gen_games_season()

            pipeline = [{"$match": {}}, {"$group": {"_id": "$tag"}}]
            all_tags = [x["_id"] for x in (await self.async_mongo.basic_clan.aggregate(pipeline).to_list(length=None))]

            pipeline = [
                {"$match": {"$and": [{"data.season": season}, {"data.state": "ended"}]}},
                {"$group": {"_id": "$data.clans.tag"}},
            ]
            done_for_this_season = [
                x["_id"] for x in (await self.async_mongo.cwl_group.aggregate(pipeline).to_list(length=None))
            ]
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
            x["_id"] for x in (await self.async_mongo.global_clans.aggregate(pipeline_4).to_list(length=None))
        ]
        existing_clans = set(existing_clans)
        # Find clans in the combined set but not in existing_clans
        new_clans = combined_set - existing_clans
        print(len(new_clans))
        return
        tags_to_add = []
        for clan in new_clans:
            tags_to_add.append(InsertOne({"tag": clan}))
        results = await self.async_mongo.global_clans.bulk_write(tags_to_add, ordered=False)
        print(results.bulk_api_result)



    async def migrate_clan_stats(self):
        clan_stats = self.db_client.clan_stats

        new_clan_stats = self.db_client.looper.get_collection("player_stats")

        docs_to_insert = []
        total_inserted = 0
        for doc in clan_stats.find({}, {"_id": 0}):
            doc: dict
            clan_tag = doc.pop("tag")
            for season, clan_season_stats in doc.items():
                season: str
                member_stats: dict
                for member_tag, member_stats in clan_season_stats.items():
                    member_tag: str
                    member_stats: dict
                    member_stats.pop("name", None)
                    member_stats.pop("townhall", None)
                    new_member_stats = member_stats | {"clan_tag": clan_tag, "season": season, "tag": member_tag}
                    docs_to_insert.append(InsertOne(new_member_stats))

            if len(docs_to_insert) > 25000:
                total_inserted += 25000
                print(f"TOTAL INSERTED: {total_inserted}")
                new_clan_stats.bulk_write(docs_to_insert, ordered=False)
                docs_to_insert = []

        if docs_to_insert:
            print(f"DONE ||| TOTAL INSERTED: {total_inserted}")
            new_clan_stats.bulk_write(docs_to_insert, ordered=False)

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
            {"$or": [{"members": {"$lt": 10}}, {"level": {"$lt": 3}}, {"capitalLeague": "Unranked"}]},
            {"$set": {"active": False}},
        )

        self.mongo.global_clans.update_many(
            {"$nor": [{"members": {"$lt": 10}}, {"level": {"$lt": 3}}, {"capitalLeague": "Unranked"}]},
            {"$set": {"active": True}},
        )

    async def better_war_tracking(self):
        right_now = pend.now(tz=pend.UTC).timestamp()
        one_week_ago = int(right_now) - (604800 * 2)

        pipeline = [
            {"$match": {"$and": [{"endTime": {"$gte": one_week_ago}}, {"type": {"$ne": "cwl"}}]}},
            {"$group": {"_id": "$clans"}},
        ]
        results = self.mongo.clan_wars.aggregate(pipeline).to_list(length=None)
        clan_tags = []
        for result in results:
            clan_tags.extend(result.get("_id", []))

    async def count_range(self):
        past = pend.now(tz=pend.UTC).subtract(days=7).int_timestamp
        pipeline = [
            {"$match": {"$and": [{"endTime": {"$gte": past}}, {"type": {"$ne": "cwl"}}]}},
            {"$group": {"_id": "$clans"}},
            {"$count": "count"},
        ]
        result = self.mongo.clan_wars.aggregate(pipeline).to_list(length=None)
        print(result)

    async def add_permanent_schedules(self):
        self.scheduler.add_job(
            self.store_all_leaderboards,
            CronTrigger(hour=4, minute=56),
            name="Store All Leaderboards",
            misfire_grace_time=300,
        )
        self.scheduler.add_job(
            self.store_legends, CronTrigger(day="*", hour=5, minute=56), name="Store Legends",
            misfire_grace_time=300
        )
        self.scheduler.add_job(
            self.store_cwl_wars,
            CronTrigger(day="13", hour=19, minute=37),
            name="Store CWL Wars",
            misfire_grace_time=300,
        )
        self.scheduler.add_job(
            self.store_cwl_groups,
            CronTrigger(day="9-12", hour="*", minute=35),
            name="Store CWL Groups",
            misfire_grace_time=300,
        )
        self.scheduler.add_job(
            self.update_autocomplete, IntervalTrigger(minutes=30), name="Update Autocomplete",
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

    async def build_ranking(self):
        ranking_pipeline = [
            # 1) unwind the now‐nested memberList
            {"$unwind": "$data.memberList"},
            # 2) filter to only Legend League entries
            {"$match": {"data.memberList.league": "Legend League"}},
            # 3) project the fields we care about, plus a composite sort key
            {
                "$project": {
                    "name": "$data.memberList.name",
                    "tag": "$data.memberList.tag",
                    "trophies": "$data.memberList.trophies",
                    "townhall": "$data.memberList.townhall",
                    "sort_field": {"trophies": "$data.memberList.trophies", "tag": "$data.memberList.tag"},
                }
            },
            # 4) drop the original _id
            {"$unset": ["_id"]},
            # 5) window‐function to compute rank over our sort_field
            {"$setWindowFields": {"sortBy": {"sort_field": -1}, "output": {"rank": {"$rank": {}}}}},
            # 6) clean up the temporary sort key
            {"$unset": ["sort_field"]},
            # 7) write results out to your new collection
            {"$out": {"db": "new_looper", "coll": "legend_rankings"}},
        ]
        await self.mongo.global_clans.aggregate(ranking_pipeline).to_list(length=None)

    async def run(self):
        try:
            await self.initialize()
            #await self.update_autocomplete()

            await self.find_new_clans()
            self.logger.info("Scheduler started. Running scheduled jobs...")
            # Keep the main thread alive
            while True:
                await asyncio.sleep(3600)  # Sleep for an hour, adjust as needed
        except (KeyboardInterrupt, SystemExit):
            self.logger.info("Shutting down scheduler...")
            self.scheduler.shutdown()



asyncio.run(ScheduledTracking().run())
