from unittest.mock import AsyncMock, MagicMock, patch

import pendulum as pend
import pytest
from freezegun import freeze_time
from loguru import logger

from bot.reminders.raid_reminder_tracker import RaidReminderTracker


@pytest.mark.asyncio
@freeze_time(
    '2025-02-02 19:00:00', tz_offset=0
)  # Sunday 19:00 UTC - 12 hours before the end of the raid
async def test_track_raid_reminders():
    logger.disable('bot.reminders.raid_reminder_tracker')  # Disable logs
    mock_db_client = MagicMock()
    mock_kafka_producer = AsyncMock()
    mock_config = MagicMock()
    mock_config.coc_client = MagicMock()

    # Mock reminders in the database
    mock_db_client.reminders.find.return_value.to_list = AsyncMock(
        return_value=[
            {
                'type': 'Clan Capital',
                'time': '12 hr',
                '_id': 'reminder_1',
                'clan': 'clan_1',
            },
            {
                'type': 'Clan Capital',
                'time': '12 hr',
                '_id': 'reminder_2',
                'clan': 'clan_2',
            },
        ]
    )

    # Mock clan members
    mock_clan = MagicMock()
    mock_clan.members = [
        MagicMock(tag='member_1', name='Alice', town_hall=13, role='leader'),
        MagicMock(tag='member_2', name='Bob', town_hall=12, role='elder'),
    ]
    mock_config.coc_client.get_clan = AsyncMock(return_value=mock_clan)

    # Create the tracker instance
    tracker = RaidReminderTracker(
        config=mock_config,
        db_client=mock_db_client,
        kafka_producer=mock_kafka_producer,
    )

    # Mock the fetch_missing_members method
    tracker.fetch_missing_members = AsyncMock(
        return_value={
            'member_1': {'name': 'Alice', 'attacks': '0', 'total_attacks': '5'}
        }
    )

    # Mock the send_to_kafka method
    tracker.send_to_kafka = AsyncMock()

    # Call the method
    await tracker.track_raid_reminders()

    # Verify reminders were retrieved
    mock_db_client.reminders.find.assert_called_once_with(
        {'type': 'Clan Capital', 'time': '12 hr'}
    )
    tracker.fetch_missing_members.assert_any_call(
        'clan_1', mock_clan.members, 0, [], []
    )
    tracker.fetch_missing_members.assert_any_call(
        'clan_2', mock_clan.members, 0, [], []
    )
    tracker.send_to_kafka.assert_awaited()
    assert (
        tracker.send_to_kafka.call_count == 2
    ), 'Not all reminders were sent to Kafka.'


@pytest.mark.asyncio
async def test_fetch_missing_members():
    mock_config = MagicMock()
    mock_config.coc_client = MagicMock()

    # Mock clan members
    mock_clan = MagicMock()
    mock_clan.members = [
        MagicMock(tag='member_1', name='Alice', town_hall=13, role='leader'),
        MagicMock(tag='member_2', name='Bob', town_hall=12, role='elder'),
    ]
    mock_config.coc_client.get_clan = AsyncMock(return_value=mock_clan)

    # Mock raid log
    mock_raid_log = MagicMock()
    mock_raid_log.members = [
        MagicMock(
            tag='member_1',
            attack_count=0,
            attack_limit=5,
            bonus_attack_limit=0,
        ),
        MagicMock(
            tag='member_2',
            attack_count=5,
            attack_limit=5,
            bonus_attack_limit=0,
        ),
    ]
    mock_get_raid_log = AsyncMock(return_value=mock_raid_log)

    tracker = RaidReminderTracker(
        config=mock_config, db_client=MagicMock(), kafka_producer=AsyncMock()
    )
    tracker._get_raid_log = mock_get_raid_log

    # Call the method
    missing_members = await tracker.fetch_missing_members(
        'clan_tag', mock_clan.members, attack_threshold=1
    )

    # Verify the results
    assert (
        'member_1' in missing_members
    ), 'member_1 should be in the missing members.'
    assert (
        'member_2' not in missing_members
    ), 'member_2 should not be in the missing members.'
    tracker._get_raid_log.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_reminder():
    # Mock configuration and dependencies
    mock_config = MagicMock()
    mock_clan = MagicMock()
    mock_clan.members = [
        MagicMock(tag='member_1', name='Alice', town_hall=13, role='leader'),
        MagicMock(tag='member_2', name='Bob', town_hall=12, role='elder'),
    ]
    mock_config.coc_client.get_clan = AsyncMock(return_value=mock_clan)

    tracker = RaidReminderTracker(
        config=mock_config, db_client=MagicMock(), kafka_producer=AsyncMock()
    )
    tracker.fetch_missing_members = AsyncMock(
        return_value={'member_1': 'details'}
    )
    tracker.send_to_kafka = AsyncMock()

    # Reminder to process
    reminder = {'clan': 'mock_clan_tag', 'type': 'Clan Capital'}

    # Call the method
    await tracker.process_reminder(reminder)

    # Verify results
    tracker.fetch_missing_members.assert_awaited_once_with(
        'mock_clan_tag', mock_clan.members, 0, [], []
    )
    tracker.send_to_kafka.assert_awaited_once_with(
        reminder, {'member_1': 'details'}
    )


@pytest.mark.asyncio
async def test_send_to_kafka():
    mock_kafka_producer = AsyncMock()

    tracker = RaidReminderTracker(
        config=MagicMock(),
        db_client=MagicMock(),
        kafka_producer=mock_kafka_producer,
    )

    reminder = {'type': 'Clan Capital', 'clan': 'clan_tag'}
    members = {'member_1': {'attacks': '0'}}

    # Call the method
    await tracker.send_to_kafka(reminder, members)

    # Verify Kafka producer behavior
    mock_kafka_producer.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_raid_weekend_loop():
    tracker = RaidReminderTracker(
        config=MagicMock(), db_client=MagicMock(), kafka_producer=AsyncMock()
    )

    # Mock methods and functions that should not run real logic
    tracker.track_raid_reminders = AsyncMock()

    # Control the loop by mocking is_raids_func()
    # We'll run exactly two iterations, then return False
    loop_side_effects = [True, True, False]
    tracker.is_raids_func = MagicMock(side_effect=loop_side_effects)

    # Patch asyncio.sleep so it doesn't block
    with patch('asyncio.sleep', new=AsyncMock()) as mock_sleep:
        # Patch pendulum.now for each iteration
        mock_times = [
            # First iteration
            pend.datetime(2025, 1, 10, 6, 0, tz='UTC'),  # start_time
            pend.datetime(2025, 1, 10, 6, 30, tz='UTC'),  # now
            pend.datetime(
                2025, 1, 10, 6, 30, tz='UTC'
            ),  # end_time of 1st iteration
            # Second iteration
            pend.datetime(2025, 1, 10, 7, 0, tz='UTC'),  # start_time
            pend.datetime(2025, 1, 10, 7, 30, tz='UTC'),  # now
            pend.datetime(
                2025, 1, 10, 7, 30, tz='UTC'
            ),  # end_time of 2nd iteration
        ]
        with patch('pendulum.now', side_effect=mock_times):
            await tracker.run()

    # track_raid_reminders should be awaited exactly twice
    assert tracker.track_raid_reminders.await_count == 2

    # is_raids_func called 3 times: two returns True, last is False
    assert tracker.is_raids_func.call_count == 3

    # asyncio.sleep was awaited twice, once per iteration
    assert mock_sleep.await_count == 2
