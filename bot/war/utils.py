from datetime import timedelta

import coc
import pendulum as pend
import ujson
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from expiring_dict import ExpiringDict
from kafka import KafkaProducer

from utility.classes import MongoDatabase

WAR_CACHE = ExpiringDict()

reminder_times = [
    f'{int(time)}hr' if time.is_integer() else f'{time}hr'
    for time in (x * 0.25 for x in range(1, 193))
]


def send_reminder(
    time: str, war_unique_id: str, clan_tag: str, producer: KafkaProducer
):
    global WAR_CACHE
    war = WAR_CACHE.get(war_unique_id)
    if war is None:
        return

    json_data = {
        'type': 'war',
        'clan_tag': clan_tag,
        'time': time,
        'data': war._raw_data,
    }
    producer.send(
        'reminder',
        ujson.dumps(json_data).encode('utf-8'),
        key=clan_tag.encode('utf-8'),
        timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000),
    )


async def clan_war_track(
    clan_tag: str,
    db_client: MongoDatabase,
    coc_client: coc.Client,
    producer: KafkaProducer,
    scheduler: AsyncIOScheduler,
):
    war = None
    try:
        war = await coc_client.get_current_war(clan_tag=clan_tag)
    except coc.errors.PrivateWarLog:
        result = (
            await db_client.clan_wars.find(
                {
                    '$and': [
                        {'clans': clan_tag},
                        {'custom_id': None},
                        {'endTime': {'$gte': pend.now(tz=pend.UTC).now()}},
                    ]
                }
            )
            .sort({'endTime': -1})
            .to_list(length=None)
        )
        if result:
            result = result[0]
            other_clan = result.get('clans', []).remove(clan_tag)
            try:
                war = await coc_client.get_current_war(clan_tag=other_clan)
            except coc.errors.PrivateWarLog:
                pass
    except Exception:
        war = None

    next_round = None
    if (
        war is not None
        and war.is_cwl
        and war.state != 'inPreparation'
        and war.league_group.state != 'ended'
    ):
        try:
            next_round = await coc_client.get_current_war(
                clan_tag=clan_tag, cwl_round=coc.WarRound.current_preparation
            )
        except Exception:
            pass

    war_list = [w for w in [war, next_round] if w is not None]

    for war in war_list:
        # notInWar state, skip
        if war.preparation_start_time is None:
            continue

        war_unique_id = (
            '-'.join([war.clan_tag, war.opponent.tag])
            + f'-{int(war.preparation_start_time.time.timestamp())}'
        )

        previous_war = WAR_CACHE.get(war_unique_id)
        if previous_war is None:
            WAR_CACHE[war_unique_id] = war

            acceptable_times = []
            for r_time in reversed(reminder_times):
                time = r_time.replace('hr', '')
                time_seconds = int(float(time) * 3600)
                if war.end_time.seconds_until >= time_seconds:
                    future_time = war.end_time.time.replace(
                        tzinfo=pend.UTC
                    ) - timedelta(seconds=time_seconds)
                    acceptable_times.append((future_time, f'{time} hr'))
            set_times = await db_client.reminders.distinct(
                'time', filter={'$and': [{'clan': clan_tag}, {'type': 'War'}]}
            )
            if acceptable_times:
                for future_time, r_time in acceptable_times:
                    if r_time in set_times:
                        scheduler.add_job(
                            send_reminder,
                            'date',
                            run_date=future_time,
                            args=[r_time, war_unique_id, clan_tag, producer],
                            id=f'war_end_{war.clan.tag}_{war.opponent.tag}_{future_time.timestamp()}',
                            misfire_grace_time=1200,
                            max_instances=1,
                        )
            continue

        if war._raw_data == previous_war._raw_data:
            continue

        WAR_CACHE[war_unique_id] = war

        if war.is_cwl and war.state == 'inPreparation':
            clan_members_added = [
                n._raw_data
                for n in war.clan.members
                if n.tag not in set(n.tag for n in previous_war.clan.members)
            ]
            clan_members_removed = [
                n._raw_data
                for n in previous_war.clan.members
                if n.tag not in set(n.tag for n in war.clan.members)
            ]

            if clan_members_added or clan_members_removed:
                json_data = {
                    'type': 'cwl_lineup_change',
                    'war': war._raw_data,
                    'league_group': league_group,
                    'added': clan_members_added,
                    'removed': clan_members_removed,
                    'clan_tag': war.clan.tag,
                }
                producer.send(
                    'war',
                    ujson.dumps(json_data).encode('utf-8'),
                    key=clan_tag.encode('utf-8'),
                    timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000),
                )

            opponent_members_added = [
                n._raw_data
                for n in war.opponent.members
                if n.tag
                not in set(n.tag for n in previous_war.opponent.members)
            ]
            opponent_members_removed = [
                n._raw_data
                for n in previous_war.opponent.members
                if n.tag not in set(n.tag for n in war.opponent.members)
            ]

            if opponent_members_added or opponent_members_removed:
                json_data = {
                    'type': 'cwl_lineup_change',
                    'war': war._raw_data,
                    'league_group': league_group,
                    'added': opponent_members_added,
                    'removed': opponent_members_removed,
                    'clan_tag': war.opponent.tag,
                }
                producer.send(
                    'war',
                    ujson.dumps(json_data).encode('utf-8'),
                    key=clan_tag.encode('utf-8'),
                    timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000),
                )

        league_group = None
        if war.is_cwl:
            league_group: coc.ClanWarLeagueGroup | None = war.league_group
            league_group = league_group._raw_data

        if previous_war.attacks:
            new_attacks = [
                a for a in war.attacks if a not in set(previous_war.attacks)
            ]
        else:
            new_attacks = war.attacks

        if new_attacks:
            json_data = {
                'type': 'new_attacks',
                'war': war._raw_data,
                'league_group': league_group,
                'attacks': [attack._raw_data for attack in new_attacks],
                'clan_tag': clan_tag,
            }
            producer.send(
                'war',
                ujson.dumps(json_data).encode('utf-8'),
                key=clan_tag.encode('utf-8'),
                timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000),
            )

        previous_league_group = None
        if previous_war.is_cwl:
            previous_league_group: coc.ClanWarLeagueGroup | None = (
                previous_war.league_group
            )
            previous_league_group = previous_league_group._raw_data

        if war.state != previous_war.state:
            json_data = {
                'type': 'war_state',
                'old_war': previous_war._raw_data,
                'new_war': war._raw_data,
                'previous_league_group': previous_league_group,
                'new_league_group': league_group,
                'clan_tag': clan_tag,
            }
            producer.send(
                'war',
                ujson.dumps(json_data).encode('utf-8'),
                key=clan_tag.encode('utf-8'),
                timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000),
            )
