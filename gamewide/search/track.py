from utility.classes import MongoDatabase
from meilisearch_python_sdk import AsyncClient
from utility.config import Config
import asyncio


async def main():
    config = Config()
    client = AsyncClient('http://85.10.200.219:7700', config.redis_pw, verify=False)
    db_client = MongoDatabase(stats_db_connection=config.stats_mongodb, static_db_connection=config.static_mongodb)

    pipeline = [{"$match": {"$nor": [{"members": {"$lt": 10}}, {"level": {"$lt": 3}}, {"capitalLeague": "Unranked"}]}}, {"$group": {"_id": "$tag"}}]
    all_tags = [x["_id"] for x in (await db_client.global_clans.aggregate(pipeline).to_list(length=None))]
    bot_clan_tags = await db_client.clans_db.distinct("tag")
    all_tags = list(set(all_tags + bot_clan_tags))
    print(f"{len(all_tags)} tags")
    size_break = 100_000
    all_tags = [all_tags[i:i + size_break] for i in range(0, len(all_tags), size_break)]

    for tag_group in all_tags:
        pipeline = [
            {"$match": {"tag": {"$in": tag_group}}},
            {"$unwind": "$memberList"},
            {
                "$project": {
                    "id": {"$substr": ["$memberList.tag", 1, {"$strLenBytes": "$memberList.tag"}]},
                    "name": "$memberList.name",
                    "clan_name": "$name",
                    "clan_tag": "$tag",
                    "townhall": "$memberList.townhall"
                }
            },
            {"$unset": ["_id"]}
        ]
        docs_to_insert = await db_client.global_clans.aggregate(pipeline=pipeline).to_list(length=None)
        print(len(docs_to_insert), "docs")

        # An index is where the documents are stored.
        index = client.index('players')
        await index.add_documents_in_batches(documents=docs_to_insert, batch_size=100_000, primary_key="id", compress=True)
        await asyncio.sleep(15)
