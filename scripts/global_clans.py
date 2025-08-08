import asyncio
import time

import pendulum as pend
from pymongo import DeleteOne, InsertOne, UpdateOne
from utility.time import gen_season_date

from .tracking import Tracking, TrackingType


class GlobalClanTracking(Tracking):
    def __init__(self):
        super().__init__(tracker_type=TrackingType.GLOBAL_CLAN_VERIFY, batch_size=25_000)

        self.season = gen_season_date()
        self.inactive_clans = []
        self.priority_clans = ...
        self.priority_players = ...
        self.clan_cache = {}

    def _clans(self, active: bool):
        pipeline = [{"$match": {"active": active}}, {"$group": {"_id": "$tag"}}]
        all_tags = [x["_id"] for x in self.mongo.all_clans.aggregate(pipeline).to_list(length=None)]
        if active:
            bot_clan_tags = self.mongo.clans_db.distinct("tag")
            all_tags = list(set(all_tags + bot_clan_tags))
        return all_tags

    def _priority_clans(self) -> set[str]:
        return set(self.mongo.clans_db.distinct("tag"))

    def _priority_players(self) -> set[str]:
        return set(self.mongo.user_settings.distinct("search.player.bookmarked"))

    def _batches(self):
        if not self.inactive_clans:
            self.inactive_clans = self._chunk_into_n(self._clans(active=False), 48)

        active_clans = self._clans(active=True)
        # this add one of the inactive clan groups to this tracking cycle
        active_clans += self.inactive_clans.pop()

        self.logger.info(f"{len(active_clans)} active clans")

        return [active_clans[i : i + self.batch_size] for i in range(0, len(active_clans), self.batch_size)]

    async def _get_previous_clans(self, clan_tags):
        stats = {}

        async def fetch_batch(batch):
            cursor = self.async_mongo.all_clans.find({"tag": {"$in": batch}})
            docs = await cursor.to_list(length=None)
            return {d["tag"]: (d["data"], d.get("records", {})) for d in docs if "data" in d}

        # split into 1k batches
        batches = [clan_tags[i : i + 500] for i in range(0, len(clan_tags), 500)]

        results = await asyncio.gather(*(fetch_batch(b) for b in batches))
        for sub in results:
            stats.update(sub)

        return stats

    def _find_join_leaves_and_donos(self, previous_clan: dict, current_clan: dict):
        changes = []
        season_stats = []
        clan_tag = current_clan.get("tag")
        now = pend.now(tz=pend.UTC)

        # Build lookup dicts keyed by player tag
        current_members = {m["tag"]: m for m in current_clan.get("memberList", [])}
        previous_members = {m["tag"]: m for m in previous_clan.get("memberList", [])}

        # Tags for comparison
        current_tags = set(current_members)
        previous_tags = set(previous_members)

        joined_tags = current_tags - previous_tags
        left_tags = previous_tags - current_tags

        # Joins
        for tag in joined_tags:
            member = current_members[tag]
            changes.append(
                InsertOne({
                    "type": "join",
                    "clan": clan_tag,
                    "time": now,
                    "tag": tag,
                    "name": member.get("name"),
                    "th": member.get("townHallLevel"),
                })
            )

        # Leaves
        for tag in left_tags:
            member = previous_members[tag]
            changes.append(
                InsertOne({
                    "type": "leave",
                    "clan": clan_tag,
                    "time": now,
                    "tag": tag,
                    "name": member.get("name"),
                    "th": member.get("townHallLevel"),
                })
            )

        if clan_tag not in self.priority_clans:
            # Donation changes
            for tag, curr in current_members.items():
                if tag in self.priority_players:
                    continue
                prev = previous_members.get(tag, {})

                donation_change = curr.get("donations", 0) - prev.get("donations", 0)
                received_change = curr.get("donationsReceived", 0) - prev.get("donationsReceived", 0)

                if donation_change > 0 or received_change > 0:
                    season_stats.append(
                        UpdateOne(
                            {"tag": tag, "season": self.season, "clan_tag": clan_tag},
                            {
                                "$inc": {
                                    "donated": donation_change,
                                    "received": received_change,
                                }
                            },
                            upsert=True
                        )
                    )

        return changes, season_stats

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
                    {"tag": current_clan.get("tag")},
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
                    {"tag": current_clan.get("tag")},
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
            return UpdateOne({"tag": new_clan.get("tag")}, {"$set" : {"data": new_clan, "records": {}}}, upsert=True)

        to_set = {}

        # compare top-level fields of new_data vs old
        for key, new_val in new_clan.items():
            old_val = previous_clan.get(key)
            if new_val != old_val:
                to_set[f"data.{key}"] = new_val

        if to_set:
            return UpdateOne({"tag": new_clan.get("tag")}, {"$set": to_set}, upsert=True)

    async def track_clans(self):
        self.logger.info("Started Loop")
        for batch in self._batches():
            self.season = gen_season_date()
            t = time.time()
            tasks = []
            for tag in batch:
                tasks.append(
                    self.fetch(
                        url=f"https://api.clashofclans.com/v1/clans/{tag.replace('#', '%23')}", tag=tag, json=True
                    )
                )

            self.logger.debug(f"pull clans API: START {time.time() - t} seconds")
            responses: list[dict] = await self._run_tasks(tasks=tasks, return_exceptions=True, wrapped=True)
            self.logger.debug(f"pull clans API: STOP {time.time() - t} seconds")

            changes = []
            join_leave_changes = []
            season_stat_changes = []
            changes_history = []

            self.logger.debug(f"pull clans DB: START {time.time() - t} seconds")
            previous_clan_batch = await self._get_previous_clans(clan_tags=batch)
            self.logger.debug(f"pull clans DB: END {time.time() - t} seconds")

            self.logger.debug(f"clan data loop: START {time.time() - t} seconds")

            self.season = gen_season_date()
            self.priority_clans = self._priority_clans()
            self.priority_players = self._priority_players()

            for clan_data in responses:
                if not isinstance(clan_data, tuple):
                    continue
                clan, clan_tag = clan_data
                if clan.get("members") == 0:
                    changes.append(DeleteOne({"_id": clan.get("tag")}))
                    continue

                previous_clan, clan_records = previous_clan_batch.get(clan_tag, ({}, {}))
                if previous_clan:
                    join_leave, season_stats = self._find_join_leaves_and_donos(previous_clan, clan)
                    join_leave_changes.extend(join_leave)
                    season_stat_changes.extend(season_stats)

                    changes_history.extend(self._find_clan_changes(previous_clan=previous_clan, current_clan=clan))
                    changes.extend(self._find_new_records(current_clan=clan, clan_records=clan_records))

                if clan_update := self._find_clan_updates(previous_clan, clan):
                    if clan_update:
                        changes.append(clan_update)

            self.logger.debug(f"clan data loop: END {time.time() - t} seconds")

            if changes:
                self.mongo.all_clans.bulk_write(changes, ordered=False)
                self.logger.info(f"Made {len(changes)} clan changes")

            if changes_history:
                self.mongo.clan_change_history.bulk_write(changes_history, ordered=False)
                self.logger.info(f"Made {len(changes_history)} clan change history")

            if join_leave_changes:
                self.mongo.join_leave_history.bulk_write(join_leave_changes, ordered=False)
                self.logger.info(f'Made {len(join_leave_changes)} join/leave changes')

            if season_stat_changes:
                self.mongo.new_player_stats.bulk_write(season_stat_changes, ordered=False)
                self.logger.info(f'Made {len(join_leave_changes)} donation changes')

            self.logger.info("batch time: ", time.time() - t, " seconds")
        self.logger.info("Finished Loop")


    async def run(self):
        await self.initialize()
        self.priority_clans = self._priority_clans()
        self.priority_players = self._priority_players()

        while True:
            await self.track_clans()
            self._submit_stats()

