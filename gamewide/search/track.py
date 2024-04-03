import asyncio

from utility.classes import MongoDatabase
from meilisearch_python_sdk import AsyncClient
from utility.config import Config
from loguru import logger

import aiohttp

async def main():
    config = Config()
    db_client = MongoDatabase(stats_db_connection=config.stats_mongodb, static_db_connection=config.static_mongodb)

    pipeline = [{"$match": {"$nor": [{"members": {"$lt": 10}}, {"level": {"$lt": 3}}, {"capitalLeague": "Unranked"}]}}, {"$group": {"_id": "$tag"}}]
    all_tags = [x["_id"] for x in (await db_client.global_clans.aggregate(pipeline).to_list(length=None))]
    bot_clan_tags = await db_client.clans_db.distinct("tag")
    all_tags = list(set(all_tags + bot_clan_tags))
    logger.info(f"{len(all_tags)} tags")
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
        logger.info(f"{len(docs_to_insert)} docs")

        # An index is where the documents are stored.

        async def add_documents(documents):
            headers = {"Authorization" : f"Bearer {config.meili_pw}"}
            async with aiohttp.ClientSession() as session:
                async with session.post('http://85.10.200.219:7700/indexes/players/documents', headers=headers, json=documents) as response:
                    if response.status == 202:  # Meilisearch accepted the update
                        #logger.info("Documents added successfully")
                        pass
                    else:
                        logger.info(f"Error adding documents. Status code: {response.status}")
                        logger.info(await response.text())

        size_break = 3_000
        all_docs = [docs_to_insert[i:i + size_break] for i in range(0, len(docs_to_insert), size_break)]
        tasks = []
        for doc_group in all_docs:
            tasks.append(asyncio.create_task(add_documents(documents=doc_group)))
        await asyncio.gather(*tasks)
        await asyncio.sleep(60)
