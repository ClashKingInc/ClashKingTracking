import asyncio
import random

import coc
import orjson
import pendulum as pend
from aiokafka import AIOKafkaConsumer
from hashids import Hashids

from .tracking import Tracking, TrackingType


class GlobalWarStore(Tracking):
    def __init__(self):
        super().__init__(tracker_type=TrackingType.GLOBAL_WAR_STORE)

        self.consumer: AIOKafkaConsumer = ...
        self.topics = ["war_store", "maintenance"]
        self._paused_jobs: dict[str, pend.Datetime] = {}

    async def _maintenance_protocol(self, duration: int):
        """
        If duration == 0: pause all jobs.
        Otherwise: shift each job’s next_run_time by `duration` seconds, then resume them.
        """
        # 1) Pause all if zero
        if duration == 0:
            now = pend.now("UTC")
            self._paused_jobs.clear()
            for job in self.scheduler.get_jobs():
                # snapshot the run time (None if it was already in the past / one-off)
                orig = job.next_run_time
                self._paused_jobs[job.id] = pend.instance(orig) if orig else None
                self.scheduler.pause_job(job.id)
            self.logger.info(f"Paused {len(self._paused_jobs)} jobs at {now.to_iso8601_string()}")
            self.is_maintenance = True
            return

        now = pend.now("UTC")
        for job_id, orig_dt in self._paused_jobs.items():
            if orig_dt:
                new_dt = orig_dt.add(seconds=duration)
            else:
                new_dt = now
            # APScheduler wants a naive datetime
            self.scheduler.reschedule_job(job_id, next_run_time=new_dt.naive())
            self.scheduler.resume_job(job_id)

        self.logger.info(
            f"Resumed {len(self._paused_jobs)} jobs with a +{duration}s shift (as of {now.to_iso8601_string()})"
        )
        self._paused_jobs.clear()

        self.is_maintenance = False

    async def store(self):
        await self.consumer.start()
        self.logger.info("Events Started")

        async for msg in self.consumer:  # type: aiokafka.structs.ConsumerRecord
            data = orjson.loads(msg.value)
            if msg.topic == "maintenance":
                await self._maintenance_protocol(duration=data.get("maintenance_duration"))

            run_time = pend.from_timestamp(timestamp=data.get("run_time"), tz=pend.UTC)
            self.scheduler.add_job(
                self.store_war,
                "date",
                run_date=run_time,
                args=[data.get("tag"), data.get("opponent_tag"), data.get("prep_time"), data.get("war_tag")],
                misfire_grace_time=None,
                max_instances=1,
            )

    async def store_war(self, clan_tag: str, opponent_tag: str, prep_time: int, war_tag: str | None):
        hashids = Hashids(min_length=7)
        war = await self._find_active_war(clan_tag, opponent_tag, prep_time, war_tag)
        if not isinstance(war, coc.ClanWar):
            return  # nothing to store

        # build our IDs exactly as before
        war_unique_id = (
            "-".join(sorted([war.clan.tag, war.opponent.tag]))
            + f"-{int(war.preparation_start_time.time.replace(tzinfo=pend.UTC).timestamp())}"
        )
        custom_id = hashids.encode(
            int(war.preparation_start_time.time.replace(tzinfo=pend.UTC).timestamp())
            + int(pend.now(tz=pend.UTC).timestamp())
            + random.randint(1_000_000_000, 9_999_999_999)
        )

        await self.mongo.clan_wars.update_one(
            {"war_id": war_unique_id},
            {"$set": {"custom_id": custom_id, "data": war._raw_data, "type": war.type}},
            upsert=True,
        )

    async def _find_active_war(
        self, clan_tag: str, opponent_tag: str, prep_time: int, war_tag: str | None
    ) -> coc.ClanWar | None | str:
        """
        Try up to 10 times to find a finished war matching `prep_time`.
        Switches to opponent_tag once if needed, handles maintenance loops,
        logs other errors, and returns:
          - a coc.ClanWar if found and prep_time matches
          - None if not found after both tags / too many tries
        """
        switched = False

        for _ in range(10):
            status = await self._safe_get_war(clan_tag, war_tag)
            if status == "maintenance":
                # wait until maintenance flag clears, then retry
                while self.is_maintenance:
                    await asyncio.sleep(5)
                continue

            if status == "no access":
                if not switched:
                    clan_tag, switched = opponent_tag, True
                    continue
                return None

            if status == "error":
                return None

            # we got a real ClanWar instance
            war: coc.ClanWar = status  # type: ignore
            if war.state == coc.enums.WarState.war_ended:
                # only accept it if the prep_time matches
                actual_prep = pend.instance(war.preparation_start_time.time).int_timestamp
                if actual_prep == prep_time:
                    return war
                # swap once if wrong prep
                if not switched:
                    clan_tag, switched = opponent_tag, True
                    continue
                return None

            # war not ended yet, back off
            await asyncio.sleep(min(war._response_retry, 120))

        return None

    async def _safe_get_war(self, clan_tag: str, war_tag: str | None) -> coc.ClanWar | str:
        """
        Wrap get_clan_war so we return:
          - coc.ClanWar on success
          - "no access" for NotFound/Forbidden/PrivateWarLog
          - "maintenance" for Maintenance
          - "error" for anything else (and logs it)
        """
        try:
            if not war_tag:
                return await self.coc_client.get_clan_war(clan_tag=clan_tag)
            return await self.coc_client.get_league_war(war_tag=war_tag)
        except (coc.NotFound, coc.errors.Forbidden, coc.errors.PrivateWarLog):
            return "no access"
        except coc.errors.Maintenance:
            return "maintenance"
        except Exception as e:
            self.logger.error(f"Unexpected error fetching war: {e}")
            return "error"

    async def run(self):
        await self.initialize()
        self.consumer: AIOKafkaConsumer = AIOKafkaConsumer(
            *self.topics, bootstrap_servers=[self.config.kafka_host], auto_offset_reset="latest"
        )
        await self.store()


asyncio.run(GlobalWarStore().run())
