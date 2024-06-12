import aiohttp
import asyncio
import coc
import orjson
import pendulum as pend
import random
import snappy
import time
import ujson

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from .capital_lb import calculate_clan_capital_leaderboards, calculate_player_capital_looted_leaderboards, calculate_raid_medal_leaderboards
from .config import GlobalScheduledConfig
from hashids import Hashids
from pymongo import UpdateOne, InsertOne
from redis import asyncio as redis
from utility.keycreation import create_keys
from utility.constants import locations
from utility.classes import MongoDatabase
from utility.utils import gen_season_date, gen_games_season, gen_raid_date

config = GlobalScheduledConfig()
db_client = MongoDatabase(stats_db_connection=config.stats_mongodb, static_db_connection=config.static_mongodb)

coc_client = coc.Client(key_count=10, throttle_limit=500, cache_max_size=0, raw_attribute=True)


async def store_clan_capital():

    async def fetch(url, session: aiohttp.ClientSession, headers, tag):
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return ((await response.json()), tag)
            return (None, None)

    pipeline = [{"$match": {}}, {"$group": {"_id": "$tag"}}]
    all_tags = [x["_id"] for x in (await db_client.global_clans.aggregate(pipeline).to_list(length=None))]
    size_break = 50000
    all_tags = [all_tags[i:i + size_break] for i in range(0, len(all_tags), size_break)]

    for tag_group in all_tags:
        tasks = []
        connector = aiohttp.TCPConnector(limit=250, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=1800)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout, json_serialize=ujson.dumps) as session:
            for tag in tag_group:
                tasks.append(fetch(f"https://api.clashofclans.com/v1/clans/{tag.replace('#', '%23')}/capitalraidseasons?limit=1", session,
                                   {"Authorization": f"Bearer {next(coc_client.http.keys)}"}, tag))
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            await session.close()

        changes = []
        responses = [r for r in responses if type(r) is tuple]
        for response, tag in responses:
            try:
                # we shouldnt have completely invalid tags, they all existed at some point
                if response is None:
                    continue
                if len(response["items"]) == 0:
                    continue
                date = coc.Timestamp(data=response["items"][0]["endTime"])
                #-3600 = 1 hour has passed
                if 60 >= date.seconds_until >= -86400:
                    changes.append(InsertOne({"clan_tag" : tag, "data" : response["items"][0]}))
            except:
                pass

        try:
            await db_client.raid_weekends.bulk_write(changes, ordered=False)
        except Exception:
            pass

    await calculate_player_capital_looted_leaderboards(db_client=db_client)
    await calculate_clan_capital_leaderboards(db_client=db_client)
    await calculate_raid_medal_leaderboards(db_client=db_client)


async def store_cwl_groups():
    season = gen_games_season()

    async def fetch(url, session: aiohttp.ClientSession, headers, tag):
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return ((await response.json()), tag)
            return (None, None)


    pipeline = [{"$match": {}}, {"$group": {"_id": "$tag"}}]
    all_tags = [x["_id"] for x in (await db_client.basic_clan.aggregate(pipeline).to_list(length=None))]

    pipeline = [{"$match": {"$and" : [{"data.season" : season}, {"data.state" : "ended"}]}}, {"$group": {"_id": "$data.clans.tag"}}]
    done_for_this_season = [x["_id"] for x in (await db_client.cwl_group.aggregate(pipeline).to_list(length=None))]
    done_for_this_season = set([j for sub in done_for_this_season for j in sub])

    all_tags = [tag for tag in all_tags if tag not in done_for_this_season]

    size_break = 50000
    all_tags = [all_tags[i:i + size_break] for i in range(0, len(all_tags), size_break)]

    was_found_in_a_previous_group = set()
    for tag_group in all_tags:
        tasks = []
        connector = aiohttp.TCPConnector(limit=250, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=1800)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout, json_serialize=ujson.dumps) as session:
            for tag in tag_group:
                if tag in was_found_in_a_previous_group:
                    continue
                tasks.append(fetch(f"https://api.clashofclans.com/v1/clans/{tag.replace('#', '%23')}/currentwar/leaguegroup", session,
                                   {"Authorization": f"Bearer {next(coc_client.http.keys)}"}, tag))
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            await session.close()

        changes = []
        responses = [r for r in responses if type(r) is tuple]
        for response, tag in responses:
            try:
                # we shouldnt have completely invalid tags, they all existed at some point
                if response is None:
                    continue
                season = response.get("season")
                for clan in response.get("clans"):
                    was_found_in_a_previous_group.add(clan.get("tag"))
                tags = sorted([clan.get("tag").replace('#','') for clan in response.get("clans")])
                cwl_id = f"{season}-{'-'.join(tags)}"
                changes.append(UpdateOne({"cwl_id" : cwl_id}, {"$set" : {"data" : response}}, upsert=True))
            except:
                pass
        if changes:
            await db_client.cwl_group.bulk_write(changes)
            print(f"{len(changes)} Changes Updated/Inserted")


async def store_cwl_wars():
    hashids = Hashids(min_length=7)
    season = gen_games_season()

    pipeline = [{"$match": {"data.season": season}},
                {"$group": {"_id": "$data.rounds.warTags"}}]
    result = await db_client.cwl_group.aggregate(pipeline).to_list(length=None)
    done_for_this_season = [x["_id"] for x in result]
    done_for_this_season = [j for sub in done_for_this_season for j in sub]
    all_tags = set([j for sub in done_for_this_season for j in sub])
    print(f"{len(all_tags)} war tags total")

    pipeline = [{"$match": {"data.season" : season}}, {"$group": {"_id": "$data.tag"}}]
    tags_already_found = set([x["_id"] for x in (await db_client.clan_wars.aggregate(pipeline).to_list(length=None))])
    print(f"{len(tags_already_found)} war tags already found")

    all_tags = [t for t in all_tags if t not in tags_already_found]
    print(f"{len(all_tags)} war tags to find")


    size_break = 60000
    all_tags = [all_tags[i:i + size_break] for i in range(0, len(all_tags), size_break)]

    async def fetch(url, session: aiohttp.ClientSession, headers, tag):
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return ((await response.read(), tag))
            return (None, None)


    for count, tag_group in enumerate(all_tags, 1):
        start_time = time.time()
        print(f"GROUP {count} | {len(tag_group)} tags")
        tasks = []
        connector = aiohttp.TCPConnector(limit=1000, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=1800)
        session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        for tag in tag_group:
            tasks.append(fetch(f"https://api.clashofclans.com/v1/clanwarleagues/wars/{tag.replace('#', '%23')}", session,
                               {"Authorization": f"Bearer {next(coc_client.http.keys)}"}, tag))
        print(f"{len(tasks)} tasks")
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        await session.close()

        print(f"{len(responses)} responses | {time.time() - start_time} sec")

        add_war = []
        responses = [(orjson.loads(r), tag) for r, tag in responses if r is not None]
        print(f"{len(responses)} valid responses | {time.time() - start_time} sec")

        for response, tag in responses: #type: dict, str
            #try:
                # we shouldnt have completely invalid tags, they all existed at some point
                response["tag"] = tag
                response["season"] = season
                war = coc.ClanWar(data=response, client=coc_client)
                if war.preparation_start_time is None:
                    continue
                custom_id = hashids.encode(
                    int(war.preparation_start_time.time.replace(tzinfo=pend.UTC).timestamp()) + int(pend.now(tz=pend.UTC).timestamp()) + random.randint(1000000000, 9999999999))
                war_unique_id = "-".join(sorted([war.clan.tag, war.opponent.tag])) + f"-{int(war.preparation_start_time.time.timestamp())}"

                add_war.append(
                    {"war_id": war_unique_id,
                        "custom_id": custom_id,
                        "data": war._raw_data,
                        "type" : "cwl"
                    }
                    )
            #except Exception:
                #pass
        print(f"working on adding | {time.time() - start_time} sec")
        if add_war:
            await db_client.clan_wars.insert_many(documents=add_war, ordered=False)
        print(f"{len(add_war)} Wars Updated/Inserted | {time.time() - start_time} sec")


async def store_all_leaderboards():

    for database, function in \
            zip([db_client.clan_trophies, db_client.clan_versus_trophies, db_client.capital, db_client.player_trophies, db_client.player_versus_trophies],
                [coc_client.get_location_clans, coc_client.get_location_clans_builder_base, coc_client.get_location_clans_capital, coc_client.get_location_players, coc_client.get_location_players_builder_base]):

        tasks = []
        for location in locations:
            task = asyncio.ensure_future(function(location_id=location))
            tasks.append(task)

        responses = await asyncio.gather(*tasks, return_exceptions=True)
        store_tasks = []
        for index, response in enumerate(responses):
            if isinstance(response, BaseException) or isinstance(response, coc.NotFound):
                continue
            location = locations[index]
            store_tasks.append(InsertOne({"location" : location,
                                          "date" : str(pend.now(tz=pend.UTC).date()),
                                          "data" : {"items": [x._raw_data for x in response]}}))

        print(store_tasks)

        #await database.bulk_write(store_tasks)


async def store_legends():
    seasons = await coc_client.get_seasons(league_id=29000022)

    seasons_present = await db_client.legend_history.distinct("season")
    missing = set(seasons) - set(seasons_present)

    for year in missing:
        after = ""
        while after is not None:
            changes = []
            async with aiohttp.ClientSession() as session:
                if after != "":
                    after = f"&after={after}"
                headers = {
                    "Accept": "application/json",
                    "authorization": f"Bearer {next(coc_client.http.keys)}"
                }
                async with session.get(f"https://api.clashofclans.com/v1/leagues/29000022/seasons/{year}?limit=100000{after}", headers=headers) as response:
                    items = await response.json()
                    players = items["items"]
                    for player in players:
                        player["season"] = year
                        changes.append(InsertOne(player))
                    try:
                        after = items["paging"]["cursors"]["after"]
                    except Exception:
                        after = None
                await session.close()

            results = await db_client.legend_history.bulk_write(changes, ordered=False)


async def update_autocomplete():
    cache = redis.Redis(host=config.redis_ip, port=6379, db=0, password=config.redis_pw, decode_responses=False, max_connections=50,
                        health_check_interval=10, socket_connect_timeout=5, retry_on_timeout=True, socket_keepalive=True)

    current_time = pend.now(tz=pend.UTC)
    one_day_ago = current_time.subtract(hours=1).timestamp()
    #any players that had an update in last 24 hours
    pipeline = [{"$match": {"last_updated" : {"$gte" : one_day_ago}}},
                {"$project": {"tag": "$tag"}},
                {"$unset": "_id"}]
    all_player_tags = [x["tag"] for x in (await db_client.player_stats.aggregate(pipeline).to_list(length=None))]

    #delete any tags that are gone
    #await db_client.player_autocomplete.delete_many({"tag" : {"$nin" : all_player_tags}})
    split_size = 50_000
    split_tags = [all_player_tags[i:i + split_size] for i in range(0, len(all_player_tags), split_size)]

    for count, group in enumerate(split_tags, 1):
        t = time.time()
        print(f"Group {count}/{len(split_tags)}")
        tasks = []
        previous_player_responses = await cache.mget(keys=group)
        for response in previous_player_responses:
            if response is not None:
                response = orjson.loads(snappy.decompress(response))
                d = {
                    "name" : response.get("name"),
                    "clan" : response.get("clan", {}).get("tag", "No Clan"),
                    "league" : response.get("league", {}).get("name", "Unranked"),
                    "tag" : response.get("tag"),
                    "th" : response.get("townHallLevel"),
                    "clan_name" : response.get("clan", {}).get("name", "No Clan"),
                    "trophies" : response.get("trophies")
                }
                tasks.append(UpdateOne({"tag" : response.get("tag")}, {"$set" : d}, upsert=True))
        print(f"starting bulk write: took {time.time() - t} secs")
        await db_client.player_autocomplete.bulk_write(tasks, ordered=False)


async def update_region_leaderboards():
    await db_client.region_leaderboard.update_many({}, {"$set": {"global_rank": None, "local_rank": None}})
    lb_changes = []
    tasks = []
    for location in locations:
        task = asyncio.ensure_future(coc_client.get_location_players(location_id=location))
        tasks.append(task)
    responses = await asyncio.gather(*tasks, return_exceptions=True)

    for index, response in enumerate(responses):
        if isinstance(response, Exception):
            continue
        location = locations[index]
        if location != "global":
            location = await coc_client.get_location(location)
        for player in response:
            player: coc.RankedPlayer
            if location == "global":
                lb_changes.append(
                    UpdateOne({"tag": player.tag}, {"$set": {f"global_rank": player.rank}}, upsert=True))
            else:
                lb_changes.append(UpdateOne({"tag": player.tag},
                                            {"$set": {f"local_rank": player.rank, f"country_name": location.name, f"country_code": location.country_code}}, upsert=True))

    if lb_changes != []:
        results = await db_client.region_leaderboard.bulk_write(lb_changes)

    await db_client.region_leaderboard.update_many({}, {"$set": {"builder_global_rank": None, "builder_local_rank": None}})
    lb_changes = []
    tasks = []
    for location in locations:
        task = asyncio.ensure_future(coc_client.get_location_players_builder_base(location_id=location))
        tasks.append(task)
    responses = await asyncio.gather(*tasks, return_exceptions=True)

    for index, response in enumerate(responses):
        if isinstance(response, Exception):
            continue
        location = locations[index]
        if location != "global":
            location = await coc_client.get_location(location)
        for player in response:
            player: coc.RankedPlayer
            if location == "global":
                lb_changes.append(
                    UpdateOne({"tag": player.tag}, {"$set": {f"builder_global_rank": player.builder_base_rank}}, upsert=True))
            else:
                lb_changes.append(UpdateOne({"tag": player.tag},
                                            {"$set": {f"builder_local_rank": player.builder_base_rank, f"country_name": location.name,
                                                      f"country_code": location.country_code}}, upsert=True))

    if lb_changes != []:
        results = await db_client.region_leaderboard.bulk_write(lb_changes)

    '''# clan changes
    await self.bot.clan_leaderboard_db.update_many({}, {"$set": {"global_rank": None, "local_rank": None}})
    lb_changes = []
    tasks = []
    for location in self.bot.locations:
        task = asyncio.ensure_future(self.bot.coc_client.get_location_clans(location_id=location))
        tasks.append(task)
    responses = await asyncio.gather(*tasks)

    for index, response in enumerate(responses):
        location = self.bot.locations[index]
        if location != "global":
            location = await self.bot.coc_client.get_location(location)
        for clan in response:
            clan: coc.RankedClan
            if location == "global":
                lb_changes.append(
                    UpdateOne({"tag": clan.tag}, {"$set": {f"global_rank": clan.rank}}, upsert=True))
            else:
                lb_changes.append(UpdateOne({"tag": clan.tag},
                                            {"$set": {f"local_rank": clan.rank, f"country_name": location.name,
                                                      f"country_code": location.country_code}}, upsert=True))

    if lb_changes != []:
        results = await self.bot.clan_leaderboard_db.bulk_write(lb_changes)'''


async def main():
    keys = await create_keys([config.coc_email.format(x=x) for x in range(config.min_coc_email, config.max_coc_email + 1)], [config.coc_password] * config.max_coc_email, as_list=True)
    await coc_client.login_with_tokens(*keys)

    '''await calculate_clan_capital_leaderboards(db_client=db_client)
    await calculate_raid_medal_leaderboards(db_client=db_client)
    return
    while True:
        await store_cwl_groups()

    await store_cwl_wars()'''

    scheduler = AsyncIOScheduler(timezone=pend.UTC)
    scheduler.add_job(store_all_leaderboards, "cron", hour=4, minute=56)
    scheduler.add_job(store_legends, "cron", day="", hour=5, minute=56)
    scheduler.add_job(store_cwl_wars, "cron", day="13", hour="19", minute=37)
    scheduler.add_job(store_cwl_groups, "cron", day="9-12", hour="*", minute=35)
    scheduler.add_job(update_autocomplete, "interval", minutes=30)
    #scheduler.add_job(store_clan_capital, "cron", day_of_week="mon", hour=10)
    scheduler.start()
