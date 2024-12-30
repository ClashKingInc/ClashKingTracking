import datetime
import coc
import pendulum as pend
import ujson
from kafka import KafkaProducer

CLAN_CACHE = {}
LAST_PRIVATE_WARLOG_WARN: dict[str, datetime.datetime] = {}

async def clan_track(
    clan_tag: str, coc_client: coc.Client, producer: KafkaProducer
):
    try:
        clan = await coc_client.get_clan(tag=clan_tag)
    except Exception:
        return

    previous_clan: coc.Clan = CLAN_CACHE.get(clan.tag)
    CLAN_CACHE[clan.tag] = clan

    if previous_clan is None:
        return None

    if not clan.public_war_log:
        if LAST_PRIVATE_WARLOG_WARN.get(clan.tag) is None:
            LAST_PRIVATE_WARLOG_WARN[clan.tag] = pend.now(tz=pend.UTC)
        elif (
            pend.now(tz=pend.UTC) - LAST_PRIVATE_WARLOG_WARN.get(clan.tag)
        ).hours >= 12:
            LAST_PRIVATE_WARLOG_WARN[clan.tag] = pend.now(tz=pend.UTC)
            json_data = {'type': 'war_log_closed', 'clan': clan._raw_data}
            producer.send(
                'clan',
                value=ujson.dumps(json_data).encode('utf-8'),
                key=clan_tag.encode('utf-8'),
                timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000),
            )

    attributes = [
        'level',
        'type',
        'description',
        'location',
        'capital_league',
        'required_townhall',
        'required_trophies',
        'war_win_streak',
        'war_league',
        'member_count',
    ]
    changed_attributes = []
    for attribute in attributes:
        if getattr(clan, attribute) != getattr(previous_clan, attribute):
            changed_attributes.append(attribute)
    if changed_attributes:
        json_data = {
            'types': changed_attributes,
            'old_clan': previous_clan._raw_data,
            'new_clan': clan._raw_data,
        }
        producer.send(
            'clan',
            value=ujson.dumps(json_data).encode('utf-8'),
            key=clan_tag.encode('utf-8'),
            timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000),
        )

    members_joined = [
        n._raw_data
        for n in clan.members
        if n.tag not in set(n.tag for n in previous_clan.members)
    ]
    members_left = [
        n._raw_data
        for n in previous_clan.members
        if n.tag not in set(n.tag for n in clan.members)
    ]
    if members_joined or members_left:
        json_data = {
            'type': 'members_join_leave',
            'old_clan': previous_clan._raw_data,
            'new_clan': clan._raw_data,
            'joined': members_joined,
            'left': members_left,
        }
        producer.send(
            'clan',
            ujson.dumps(json_data).encode('utf-8'),
            key=clan_tag.encode('utf-8'),
            timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000),
        )

    previous_donations = {
        n.tag: (n.donations, n.received) for n in previous_clan.members
    }
    for member in clan.members:
        if (member_donated := previous_donations.get(member.tag)) is not None:
            mem_donos, mem_received = member_donated
            if mem_donos < member.donations or mem_received < member.received:
                json_data = {
                    'type': 'all_member_donations',
                    'old_clan': previous_clan._raw_data,
                    'new_clan': clan._raw_data,
                }
                producer.send(
                    'clan',
                    ujson.dumps(json_data).encode('utf-8'),
                    key=clan_tag.encode('utf-8'),
                    timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000),
                )
                break
