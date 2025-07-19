import asyncio

import pendulum as pend
from loguru import logger
from pymongo import DeleteOne, InsertOne, UpdateOne

from tracking import Tracking, TrackingType


class GlobalClanTracking(Tracking):
    def __init__(self):
        super().__init__(tracker_type=TrackingType.GLOBAL_CLAN_VERIFY, batch_size=25_000)

        self.inactive_clans = []

        self.clan_cache = {}

    def _clans(self, active: bool):
        pipeline = [{"$match": {"active": active}}, {"$group": {"_id": "$tag"}}]
        all_tags = [x["_id"] for x in self.mongo.global_clans.aggregate(pipeline).to_list(length=None)]
        if active:
            bot_clan_tags = self.mongo.clans_db.distinct("tag")
            all_tags = list(set(all_tags + bot_clan_tags))
        return all_tags

    def _batches(self):
        if not self.inactive_clans:
            self.inactive_clans = self._chunk_into_n(self._clans(active=False), 48)

        active_clans = self._clans(active=True)
        # this add one of the inactive clan groups to this tracking cycle
        active_clans += self.inactive_clans.pop()

        logger.info(f"{len(active_clans)} active clans")

        return [active_clans[i : i + self.batch_size] for i in range(0, len(active_clans), self.batch_size)]

    async def _get_previous_clans(self, clan_tags):
        stats = {}

        async def fetch_batch(batch):
            cursor = self.async_mongo.all_clans.find({"data.tag": {"$in": batch}})
            docs = await cursor.to_list(length=None)
            return {d["_id"]: (d["data"], d.get("records", {})) for d in docs}

        # split into 1k batches
        batches = [clan_tags[i : i + 500] for i in range(0, len(clan_tags), 500)]

        results = await asyncio.gather(*(fetch_batch(b) for b in batches))
        for sub in results:
            stats.update(sub)

        return stats

    def _find_join_leaves(self, previous_clan: dict, current_clan: dict):
        changes = []
        clan_tag = current_clan.get("tag")

        current_members = current_clan.get("memberList", [])
        previous_members = previous_clan.get("memberList", [])

        new_joins = [
            player for player in current_members if player.get("tag") not in set(p.get("tag") for p in previous_members)
        ]
        new_leaves = [
            player for player in previous_members if player.get("tag") not in set(p.get("tag") for p in current_members)
        ]
        for join in new_joins:
            changes.append(
                InsertOne(
                    {
                        "type": "join",
                        "clan": clan_tag,
                        "time": pend.now(tz=pend.UTC),
                        "tag": join.get("tag"),
                        "name": join.get("name"),
                        "th": join.get("townHallLevel"),
                    }
                )
            )

        for leave in new_leaves:
            changes.append(
                InsertOne(
                    {
                        "type": "leave",
                        "clan": clan_tag,
                        "time": pend.now(tz=pend.UTC),
                        "tag": leave.get("tag"),
                        "name": leave.get("name"),
                        "th": leave.get("townHallLevel"),
                    }
                )
            )
        return changes

    def _find_clan_changes(self, previous_clan: dict, current_clan: dict):
        changes = []
        if previous_clan.get("description") != current_clan.get("description"):
            changes.append(
                InsertOne(
                    {
                        "type": "description",
                        "clan": current_clan.get("tag"),
                        "previous": previous_clan.get("description"),
                        "current": current_clan.get("description"),
                        "time": pend.now(tz=pend.UTC).int_timestamp,
                    }
                )
            )

        if previous_clan.get("clanLevel") != current_clan.get("clanLevel"):
            changes.append(
                InsertOne(
                    {
                        "type": "clan_level",
                        "clan": current_clan.get("tag"),
                        "previous": previous_clan.get("clanLevel"),
                        "current": current_clan.get("clanLevel"),
                        "time": pend.now(tz=pend.UTC).int_timestamp,
                    }
                )
            )
        return changes

    def _find_new_records(self, current_clan: dict, clan_records: dict):
        changes = []
        if current_clan.get("warWinStreak") > clan_records.get("warWinStreak", {}).get("value", 0):
            changes.append(
                UpdateOne(
                    {"data.tag": current_clan.get("tag")},
                    {
                        "$set": {
                            "records.warWinStreak": {
                                "value": current_clan.get("warWinStreak"),
                                "time": pend.now(tz=pend.UTC).int_timestamp,
                            }
                        }
                    },
                )
            )

        if current_clan.get("clanPoints") > clan_records.get("clanPoints", {}).get("value", 0):
            changes.append(
                UpdateOne(
                    {"data.tag": current_clan.get("tag")},
                    {
                        "$set": {
                            "records.clanPoints": {
                                "value": current_clan.get("clanPoints"),
                                "time": pend.now(tz=pend.UTC).int_timestamp,
                            }
                        }
                    },
                )
            )
        return changes

    def _find_clan_updates(self, previous_clan: dict, new_clan: dict):
        if not previous_clan:
            return InsertOne({"_id": new_clan.get("tag"), "data": new_clan, "records": {}})

        to_set = {}

        # compare top-level fields of new_data vs old
        for key, new_val in new_clan.items():
            old_val = previous_clan.get(key)
            if new_val != old_val:
                to_set[f"data.{key}"] = new_val

        if to_set:
            return UpdateOne({"data.tag": new_clan.get("tag")}, {"$set": to_set}, upsert=True)

    async def track_clans(self):
        import time

        for batch in self._batches():
            t = time.time()
            tasks = []
            for tag in batch:
                tasks.append(
                    self.fetch(
                        url=f"https://api.clashofclans.com/v1/clans/{tag.replace('#', '%23')}", tag=tag, json=True
                    )
                )

            print(f"pull clans API: START {time.time() - t} seconds")
            clan_data: list[dict] = await self._run_tasks(tasks=tasks, return_exceptions=True, wrapped=True)
            print(f"pull clans API: STOP {time.time() - t} seconds")
            print(len([c for c in clan_data if c is not None]), "VALID CLANS")

            changes = []
            join_leave_changes = []
            changes_history = []

            print(f"pull clans DB: START {time.time() - t} seconds")
            previous_clan_batch = await self._get_previous_clans(clan_tags=batch)
            print(f"pull clans DB: END {time.time() - t} seconds")

            print(f"clan data loop: START {time.time() - t} seconds")

            for clan, clan_tag in clan_data:
                if clan is None:
                    continue

                if clan.get("members") == 0:
                    changes.append(DeleteOne({"_id": clan.get("tag")}))
                    continue

                previous_clan, clan_records = previous_clan_batch.get(clan_tag, (None, None))
                if previous_clan:
                    join_leave_changes.extend(self._find_join_leaves(previous_clan, clan))
                    changes_history.extend(self._find_clan_changes(previous_clan=previous_clan, current_clan=clan))
                    changes.extend(self._find_new_records(current_clan=clan, clan_records=clan_records))

                if clan_update := self._find_clan_updates(previous_clan, clan):
                    if clan_update:
                        changes.append(clan_update)

            print(f"clan data loop: END {time.time() - t} seconds")

            if changes:
                try:
                    self.mongo.all_clans.bulk_write(changes, ordered=False)
                except Exception as e:
                    print(e)
                logger.info(f"Made {len(changes)} clan changes")

            if changes_history:
                self.mongo.clan_change_history.bulk_write(changes_history, ordered=False)
                logger.info(f"Made {len(changes_history)} clan change history")

            print("batch time: ", time.time() - t, " seconds")
            """if join_leave_changes:
                await db_client.join_leave_history.bulk_write(
                    join_leave_changes, ordered=False
                )
                logger.info(
                    f'Made {len(join_leave_changes)} join/leave changes'
                )"""

    async def update_mongo_cache(self):
        pass

    async def run(self):
        await self.initialize()
        while True:
            await self.track_clans()
            # LETS STORE THE CWL & RAID LEAGUE CHANGES ELSEWHERE, SCHEDULED


asyncio.run(GlobalClanTracking().run())
