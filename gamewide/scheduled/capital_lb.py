import pendulum as pend

from pymongo import UpdateOne
from utility.classes import MongoDatabase
from utility.utils import gen_raid_date
from utility.constants import leagues

async def calculate_player_capital_looted_leaderboards(db_client: MongoDatabase):
    weekend = pend.parse(gen_raid_date(), strict=False).replace(hour=7).subtract(days=7)

    store_weekend = weekend.format('YYYY-MM-DD')
    lookup_weekend = weekend.strftime('%Y%m%dT%H%M%S.000Z')

    for league in ["All"] + leagues:
        if league == "Unranked":
            continue
        if league == "All":
            condition = {}
        else:
            condition = {"changes.old_league": {"$eq": league}}
        pipeline = [
            {
                '$match': {'data.startTime': lookup_weekend}
            },
            {"$match": condition},
            {"$unwind": "$data.members"},
            {"$project": {
                "tag": "$data.members.tag",
                "name": "$data.members.name",
                "looted": "$data.members.capitalResourcesLooted",
                "clan_tag": "$clan_tag",
                "league": "$changes.old_league"
            }},
            {"$sort": {"looted": -1}},
            {"$limit": 1000},
            {"$unset": "_id"}
        ]
        results = await db_client.raid_weekends.aggregate(pipeline=pipeline).to_list(length=None)
        for rank, result in enumerate(results, 1):
            result["type"] = "capital_looted"
            result["weekend"] = store_weekend
            result["ranking"] = {"league": league, "rank": rank}

        await db_client.player_capital_lb.insert_many(documents=results, ordered=False)


async def calculate_clan_capital_leaderboards(db_client: MongoDatabase):

    await db_client.clan_capital_lb.delete_many({})

    '''weekend = pend.parse(gen_raid_date(), strict=False).replace(hour=7).subtract(days=7)

    next_week = pend.parse(gen_raid_date(), strict=False).replace(hour=7)

    store_weekend = weekend.format('YYYY-MM-DD')
    next_store_weekend = next_week.format('YYYY-MM-DD')

    lookup_weekend = weekend.strftime('%Y%m%dT%H%M%S.000Z')

    pipeline = [{"$match": {'data.startTime': lookup_weekend}}, {"$group": {"_id": "$clan_tag"}}]
    clans_that_did_raid_weekend = [x["_id"] for x in (await db_client.raid_weekends.aggregate(pipeline).to_list(length=None))]

    print(len(clans_that_did_raid_weekend), "clans did raid weekend")
    size_break = 100_000
    all_tags = [clans_that_did_raid_weekend[i:i + size_break] for i in range(0, len(clans_that_did_raid_weekend), size_break)]

    leagues = []
    for group in all_tags:
        l = await db_client.global_clans.find({"tag": {"$in": group}}, {f'changes.clanCapital.{next_store_weekend}.league': 1,
                                                                      f'changes.clanCapital.{store_weekend}.league': 1,
                                                                      f'changes.clanCapital.{next_store_weekend}.trophies': 1,
                                                                      f'changes.clanCapital.{store_weekend}.trophies': 1,
                                                                      "tag": 1}).to_list(length=None)
        leagues += l

    print(len(leagues), "leagues found")
    changes = []
    for clan in leagues:
        if clan.get("changes", {}).get("clanCapital", {}).get(store_weekend, {}).get("league", None) is not None:
            changes.append(UpdateOne({"$and": [{"clan_tag": clan.get("tag")}, {'data.startTime': lookup_weekend}]},
                                     {"$set": {"changes": {"old_league": clan.get("changes", {}).get("clanCapital", {}).get(store_weekend, {}).get("league", None),
                                                           "new_league": clan.get("changes", {}).get("clanCapital", {}).get(next_store_weekend, {}).get("league", None),
                                                           "old_trophies": clan.get("changes", {}).get("clanCapital", {}).get(store_weekend, {}).get("trophies", 0),
                                                           "new_trophies": clan.get("changes", {}).get("clanCapital", {}).get(next_store_weekend, {}).get("trophies", 0)
                                                           }}}))
    print(len(changes), "changes")
    await db_client.raid_weekends.bulk_write(changes, ordered=False)
    print("done")'''

    for w in ["2024-05-10", "2024-05-17"]:
        print("WEEEKEND CHANGE", w)
        weekend = pend.parse(w, strict=False).replace(hour=7).subtract(days=7)

        store_weekend = weekend.format('YYYY-MM-DD')
        lookup_weekend = weekend.strftime('%Y%m%dT%H%M%S.000Z')

        for sort_type in ["capitalTotalLoot", "raidsCompleted", "enemyDistrictsDestroyed"]:
            for league in ["All"] + leagues:
                if league == "Unranked":
                    continue
                if league == "All":
                    condition = {}
                else:
                    condition = {"changes.old_league": {"$eq": league}}
                pipeline = [
                    {'$match': {'data.startTime': lookup_weekend}},
                    {"$match": condition},
                    {"$sort": {f"data.{sort_type}": -1}},
                    {"$limit": 1000},
                    {"$project": {
                        "tag": "$clan_tag",
                        "details.capitalTotalLoot": "$data.capitalTotalLoot",
                        "details.raidsCompleted": "$data.raidsCompleted",
                        "details.districtsDestroyed": "$data.enemyDistrictsDestroyed",
                        "details.totalAttacks": "$data.totalAttacks",
                        "league": "$changes.old_league"
                    }},
                    {"$unset": "_id"}
                ]
                print(pipeline)
                results = await db_client.raid_weekends.aggregate(pipeline=pipeline).to_list(length=None)
                clans = await db_client.global_clans.find({"tag" : {"$in" : [x.get("tag") for x in results]}}, {"name" : 1, "tag" : 1}).to_list(length=None)
                name_map = {x.get("tag") : x.get("name") for x in clans}

                for rank, result in enumerate(results, 1):
                    result["name"] = name_map.get(result.get("tag"))
                    result["type"] = sort_type
                    result["weekend"] = store_weekend
                    result["ranking"] = {"league" : league, "rank" : rank}

                await db_client.clan_capital_lb.insert_many(documents=results, ordered=False)


async def calculate_raid_medal_leaderboards(db_client: MongoDatabase):
    weekend = pend.parse("2024-05-10", strict=False).replace(hour=7).subtract(days=7)

    store_weekend = weekend.format('YYYY-MM-DD')
    lookup_weekend = weekend.strftime('%Y%m%dT%H%M%S.000Z')

    for league in ["All"] + leagues:
        if league == "Unranked":
            continue
        if league == "All":
            condition = {}
        else:
            condition = {"changes.old_league": {"$eq": league}}
        pipeline = [
            {'$match': {'data.startTime': lookup_weekend}},
            {"$match": condition},
            {"$addFields" : {"medals" : {"$add" : [{"$multiply" : ["$data.offensiveReward", 6]}, "$data.defensiveReward"]}}},
            {"$sort": {"medals": -1}},
            {"$limit": 1_000},
            {"$project": {
                "tag": "$clan_tag",
                "medals": "$medals",
                "details.capitalTotalLoot" : "$data.capitalTotalLoot",
                "details.raidsCompleted": "$data.raidsCompleted",
                "details.districtsDestroyed": "$data.enemyDistrictsDestroyed",
                "details.totalAttacks": "$data.totalAttacks",
                "league": "$changes.old_league"
            }},
            {"$unset": "_id"}
        ]
        results = await db_client.raid_weekends.aggregate(pipeline=pipeline).to_list(length=None)
        clans = await db_client.global_clans.find({"tag" : {"$in" : [x.get("tag") for x in results]}}, {"name" : 1, "tag" : 1}).to_list(length=None)
        name_map = {x.get("tag") : x.get("name") for x in clans}

        for rank, result in enumerate(results, 1):
            result["name"] = name_map.get(result.get("tag"))
            result["type"] = "medals"
            result["weekend"] = store_weekend
            result["ranking"] = {"league" : league, "rank" : rank}

        await db_client.clan_capital_lb.insert_many(documents=results, ordered=False)


