import asyncio
import random

import coc
import pendulum as pend
from aiokafka import AIOKafkaConsumer
from hashids import Hashids
import orjson
from .tracking import Tracking, TrackingType


class GlobalWarStore(Tracking):
    def __init__(self):
        super().__init__(tracker_type=TrackingType.GLOBAL_WAR_STORE)

        self.consumer: AIOKafkaConsumer = ...
        self.topics = ["war_store"]

    async def store(self):
        topics = ['war_store']
        consumer: AIOKafkaConsumer = AIOKafkaConsumer(
            *topics, bootstrap_servers='85.10.200.219:9092', auto_offset_reset='latest'
        )
        await consumer.start()
        self.logger.info('Events Started')
        async for msg in consumer:
            msg = orjson.loads(msg.value)
            run_time = pend.from_timestamp(timestamp=msg.get('run_time'), tz=pend.UTC)
            try:
                self.scheduler.add_job(
                    self.store_war,
                    'date',
                    run_date=run_time,
                    args=[msg.get('tag'), msg.get('opponent_tag'), msg.get('prep_time')],
                    misfire_grace_time=None,
                    max_instances=1,
                )
            except Exception:
                pass

    async def get_war(self, clan_tag: str):
        try:
            war = await self.coc_client.get_clan_war(clan_tag=clan_tag)
            return war
        except (coc.NotFound, coc.errors.Forbidden, coc.errors.PrivateWarLog):
            return 'no access'
        except coc.errors.Maintenance:
            return 'maintenance'
        except Exception as e:
            self.logger.error(str(e))
            return 'error'

    async def find_active_war(self, clan_tag: str, opponent_tag: str, prep_time: int):
        switched = False
        errors = 0
        while True:
            war = await self.get_war(clan_tag=clan_tag)
            if isinstance(war, coc.ClanWar):
                if war.state == 'warEnded':
                    return war  # Found the completed war

                # Check prep time and retry if needed
                if (war.preparation_start_time is None
                        or int(war.preparation_start_time.time.replace(tzinfo=pend.UTC).timestamp()) != prep_time):
                    if not switched:
                        clan_tag = opponent_tag
                        switched = True
                    else:
                        return "no access"  # Both tags checked, no valid war found

                if war.state == coc.enums.WarState.in_war:
                    await asyncio.sleep(min(war._response_retry, 120))
            elif war == 'maintenance':
                await asyncio.sleep(15 * 60)  # Wait 15 minutes for maintenance, then continue loop
            elif war == 'error':
                errors += 1
                if errors == 10:
                    return "unknown error"
                continue  # Stop on error
            elif war == 'no access':
                if not switched:
                    clan_tag = opponent_tag
                    switched = True
                else:
                    return "no_access"  # Both tags checked, no access to either

    async def store_war(self, clan_tag: str, opponent_tag: str, prep_time: int):
        hashids = Hashids(min_length=7)

        war = await self.find_active_war(clan_tag=clan_tag, opponent_tag=opponent_tag, prep_time=prep_time)

        war_unique_id = '-'.join(sorted([clan_tag, opponent_tag])) + f'-{prep_time}'

        if isinstance(war, str):
            await self.async_mongo.clan_wars.update_one(
                {'war_id': war_unique_id},
                {'$set': {'failure_reason': war}},
            )
            return

        custom_id = hashids.encode(
            int(prep_time) + pend.now(tz=pend.UTC).int_timestamp + random.randint(1000000000, 9999999999)
        )
        await self.async_mongo.clan_wars.update_one(
            {'war_id': war_unique_id},
            {'$set': {'custom_id': custom_id, 'data': war._raw_data, 'type': war.type}},
            upsert=True,
        )

    async def run(self):
        await self.initialize()
        self.scheduler.start()
        self.consumer: AIOKafkaConsumer = AIOKafkaConsumer(
            *self.topics, bootstrap_servers=self.config.kafka_host, auto_offset_reset="latest"
        )
        await self.store()


