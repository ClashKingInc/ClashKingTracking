from typing import List, Optional

import coc
import pendulum as pend
from aiohttp import ContentTypeError
from expiring_dict import ExpiringDict
from msgspec import Struct
from msgspec.json import decode
from pymongo import InsertOne, UpdateOne

from .tracking import Tracking, TrackingType


class Members(Struct):
    tag: str


class Clan(Struct):
    tag: Optional[str] = None
    members: List[Members] = None


class War(Struct):
    state: str
    clan: Clan
    opponent: Clan
    preparationStartTime: str = None
    startTime: str = None
    endTime: str = None


class GlobalWarTrack(Tracking):
    def __init__(self):
        super().__init__(tracker_type=TrackingType.GLOBAL_WAR, batch_size=50_000)
        self.CLANS_IN_WAR = ExpiringDict()
        self.GROUP_IN_WAR = ExpiringDict()
        self.inactive_clans = []
        self.captured_timers = set()

    def _active_clans(self):
        two_weeks_ago = pend.now(tz=pend.UTC).subtract(days=14).int_timestamp

        pipeline = [
            {"$match": {"$and": [{"endTime": {"$gte": two_weeks_ago}}, {"type": {"$ne": "cwl"}}]}},
            {"$unwind": "$clans"},
            {"$group": {"_id": "$clans"}},
        ]
        active_clans = [x["_id"] for x in self.mongo.clan_wars.aggregate(pipeline).to_list(length=None)]
        bot_clan_tags = self.mongo.clans_db.distinct("tag")
        active_clans = list(set(active_clans + bot_clan_tags))

        return active_clans

    def _open_war_log_clans(self):
        pipeline = [{"$match": {"data.isWarLogPublic": True}}, {"$group": {"_id": "$tag"}}]
        all_tags = [
            x["_id"]
            for x in self.mongo.all_clans.aggregate(pipeline, hint={"data.isWarLogPublic": 1, "tag": 1}).to_list(
                length=None
            )
        ]
        return all_tags

    def _all_clans_batched(self):
        pipeline = [{"$match": {}}, {"$group": {"_id": "$tag"}}]
        all_tags = [x["_id"] for x in self.mongo.all_clans.aggregate(pipeline).to_list(length=None)]
        bot_clan_tags = self.mongo.clans_db.distinct("tag")
        all_tags = list(set(all_tags + bot_clan_tags))

        return [all_tags[i : i + self.batch_size] for i in range(0, len(all_tags), self.batch_size)]

    def _batches(self):
        active_clans = self._active_clans()

        if not self.inactive_clans:
            openlog_clans = self._open_war_log_clans()
            active_set = set(active_clans)
            inactive_clans = list(set(openlog_clans) - active_set)
            self.inactive_clans = self._chunk_into_n(inactive_clans, 24)
            if self._cycle_count != 0:
                self.captured_timers.clear()

        # this add one of the inactive clan groups to this tracking cycle
        active_clans += self.inactive_clans.pop()

        self.logger.info(f"{len(active_clans)} clans to track")

        active_clans = [clan for clan in active_clans if clan not in self.CLANS_IN_WAR]
        self.logger.info(f"{len(active_clans)} of those clans not in war")

        return [active_clans[i : i + self.batch_size] for i in range(0, len(active_clans), self.batch_size)]

    def _war_timer_changes(self, war: War | coc.ClanWar):
        timers = []
        for member in war.clan.members + war.opponent.members:
            if isinstance(war, coc.ClanWar):
                time = pend.instance(war.end_time.time, tz=pend.UTC)
                tag = f"cwl-{member.tag}"
            else:
                regular_war_prep_time = 23 * 60 * 60
                start_time = pend.parse(war.startTime)
                prep_time = pend.parse(war.preparationStartTime)
                if start_time.diff(prep_time).in_seconds() == regular_war_prep_time:
                    tag = member.tag
                else:
                    tag = f"friendly-{member.tag}"
                time = pend.parse(war.endTime)
            timers.append(
                UpdateOne(
                    {"_id": tag}, {"$set": {"clans": [war.clan.tag, war.opponent.tag], "time": time}}, upsert=True
                )
            )
        return timers

    def timers_already_captured(self):
        pipeline = [
            {"$match": {"endTime": {"$gte": pend.now(tz=pend.UTC).subtract(weeks=1).int_timestamp}}},
            {"$match": {"data": None}},
            {"$project": {"war_id": 1}},
        ]
        result = self.mongo.clan_wars.aggregate(pipeline).to_list(length=None)
        war_ids = [x["war_id"] for x in result]
        return set(war_ids)

    async def _maintenance_protocol(self):
        maintenance_time = await self._check_maintenance()
        if maintenance_time:
            json_data = {"maintenance_status": "end", "maintenance_duration": maintenance_time}
            self._send_to_kafka("maintenance", json_data, None)
            self.mongo.war_timer.update_many(
                {}, [{"$set": {"time": {"$dateAdd": {"startDate": "$time", "unit": "second", "amount": 2}}}}]
            )

    async def _war_track(self):
        if self._cycle_count == 0:
            self.captured_timers = self.timers_already_captured()
            self.logger.debug(f"Captured {len(self.captured_timers)} timers")

        batches = self._batches()
        for count, batch in enumerate(batches, start=1):
            await self._maintenance_protocol()
            self.logger.debug(f"Starting Cycle {self._cycle_count} | Batch {count}/{len(batches)}")
            tasks = [
                self.fetch(
                    url=f"https://api.clashofclans.com/v1/clans/{tag.replace('#', '%23')}/currentwar",
                    tag=tag,
                    json=False,
                )
                for tag in batch
                if tag not in self.CLANS_IN_WAR
            ]

            wars = await self._run_tasks(tasks=tasks, return_exceptions=True, wrapped=True)
            self.logger.debug(f"Pulled {len(wars)} wars")

            changes = []
            war_timers = []
            for war_data in wars:
                if isinstance(war_data, coc.Maintenance):
                    break
                elif isinstance(war_data, coc.ClashOfClansException):
                    continue
                elif isinstance(war_data, ContentTypeError):
                    self.logger.debug("Error fetching data")
                    continue

                war, clan_tag = war_data

                war = decode(war, type=War)
                if war.preparationStartTime is None or not war.clan.members:
                    continue

                war_end = pend.parse(war.endTime)

                now = pend.now(tz=pend.UTC)
                if war_end < now:
                    continue

                war_prep = pend.parse(war.preparationStartTime)

                opponent_tag = war.opponent.tag if war.opponent.tag != clan_tag else war.clan.tag

                self.CLANS_IN_WAR.ttl(key=clan_tag, value=True, ttl=war_end.diff(now).in_seconds())
                self.CLANS_IN_WAR.ttl(key=opponent_tag, value=True, ttl=war_end.diff(now).in_seconds())

                war_unique_id = "-".join(sorted([war.clan.tag, war.opponent.tag])) + f"-{war_prep.int_timestamp}"
                if war_unique_id not in self.captured_timers:
                    war_timers.extend(self._war_timer_changes(war))
                    changes.append(
                        InsertOne(
                            {
                                "war_id": war_unique_id,
                                "clans": [clan_tag, opponent_tag],
                                "endTime": war_end.int_timestamp,
                            }
                        )
                    )

                json_data = {
                    "tag": clan_tag,
                    "opponent_tag": opponent_tag,
                    "prep_time": war_prep.int_timestamp,
                    "run_time": war_end.int_timestamp,
                }
                self._send_to_kafka(topic="war_store", data=json_data, key=None)

            if changes:
                try:
                    self.mongo.clan_wars.bulk_write(changes, ordered=False)
                except Exception:  # sometimes we will get duplicates, nothing we can do about it
                    pass

            if war_timers:
                self.mongo.war_timer.bulk_write(war_timers, ordered=False)

            self.logger.info(f"{len(self.CLANS_IN_WAR)} clans in war")

    async def run(self):
        await self.initialize()

        while True:
            await self._war_track()
            self._cycle_count += 1
