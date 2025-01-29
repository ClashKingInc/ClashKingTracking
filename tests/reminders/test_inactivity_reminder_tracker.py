from unittest.mock import AsyncMock, MagicMock

import pendulum as pend
import pytest
from freezegun import freeze_time
from loguru import logger

from bot.reminders.inactivity_reminder_tracker import InactivityReminderTracker


@pytest.mark.asyncio
async def test_fetch_reminders():
    mock_db_client = MagicMock()
    mock_db_client.reminders.find.return_value.to_list = AsyncMock(
        return_value=[{'type': 'inactivity', 'clan': 'clan_tag'}]
    )

    tracker = InactivityReminderTracker(
        config=MagicMock(),
        db_client=mock_db_client,
        kafka_producer=AsyncMock(),
    )
    reminders = await tracker.fetch_reminders()

    assert reminders == [{'type': 'inactivity', 'clan': 'clan_tag'}]
    mock_db_client.reminders.find.assert_called_once_with(
        {'type': 'inactivity'}
    )


@pytest.mark.asyncio
async def test_parse_time():
    reminder_setting = {'time': '12 hr'}
    result = await InactivityReminderTracker.parse_time(reminder_setting)

    assert result == 43200  # 12 hours in seconds

    reminder_setting = {}
    result = await InactivityReminderTracker.parse_time(reminder_setting)

    assert result is None


@pytest.mark.asyncio
async def test_fetch_inactive_members():
    mock_db_client = MagicMock()
    now = pend.now('UTC')

    mock_db_client.player_stats.find.return_value.to_list = AsyncMock(
        return_value=[
            {'name': 'Alice', 'last_online': now.int_timestamp - 7200}
        ]
    )

    tracker = InactivityReminderTracker(
        config=MagicMock(),
        db_client=mock_db_client,
        kafka_producer=AsyncMock(),
    )

    inactive_members = await tracker.fetch_inactive_members(3600, ['member_1'])

    assert inactive_members == [
        {'name': 'Alice', 'last_online': now.int_timestamp - 7200}
    ]

    mock_db_client.player_stats.find.assert_called_once()


@pytest.mark.asyncio
async def test_send_to_kafka():
    mock_kafka_producer = AsyncMock()
    mock_member = {
        'name': 'Alice',
        'last_online': 1670000000,
        '_id': 'member_1',
    }
    reminder = {'type': 'inactivity', 'clan': 'clan_tag'}

    tracker = InactivityReminderTracker(
        config=MagicMock(),
        db_client=MagicMock(),
        kafka_producer=mock_kafka_producer,
    )

    await tracker.send_to_kafka([mock_member], reminder)

    mock_kafka_producer.send.assert_awaited_once()


@pytest.mark.asyncio
@freeze_time('2025-02-02 12:00:00', tz_offset=0)  # Freeze time for consistency
async def test_process_clan():
    # Mock the config and clan details
    mock_config = MagicMock()
    mock_clan = MagicMock()
    mock_clan.members = [MagicMock(tag='member_1'), MagicMock(tag='member_2')]
    mock_config.coc_client.get_clan = AsyncMock(return_value=mock_clan)

    # Mock the database response for inactive members
    mock_db_client = MagicMock()
    mock_db_client.player_stats.find.return_value.to_list = AsyncMock(
        return_value=[
            {'name': 'Alice', 'last_online': 1670000000, '_id': 'member_1'}
        ]
    )

    # Initialize the tracker with mocked dependencies
    tracker = InactivityReminderTracker(
        config=mock_config,
        db_client=mock_db_client,
        kafka_producer=AsyncMock(),
    )
    tracker.send_to_kafka = AsyncMock()

    # Define a reminder to process
    reminder = {'type': 'inactivity', 'clan': 'clan_tag'}

    # Call the method
    await tracker.process_clan('clan_tag', [3600], reminder)

    # Verify the send_to_kafka method was called with expected arguments
    tracker.send_to_kafka.assert_awaited_once_with(
        [{'name': 'Alice', 'last_online': 1670000000, '_id': 'member_1'}],
        reminder,
    )
    mock_config.coc_client.get_clan.assert_awaited_once_with('clan_tag')

    # Calculate expected timestamps
    now = pend.now('UTC')
    lower_bound = now.subtract(seconds=3600 + 3600 - 1).int_timestamp
    upper_bound = now.subtract(seconds=3600).int_timestamp

    # Assert the database query matches expected timestamps
    mock_db_client.player_stats.find.assert_called_once_with(
        {
            'tag': {'$in': ['member_1', 'member_2']},
            'last_online': {'$gte': lower_bound, '$lt': upper_bound},
        }
    )


@pytest.mark.asyncio
@freeze_time('2025-02-02 12:00:00', tz_offset=0)
async def test_run():
    logger.disable('bot.reminders.inactivity_reminder_tracker')
    # Mock reminders in the database
    mock_db_client = MagicMock()
    mock_db_client.reminders.find.return_value.to_list = AsyncMock(
        return_value=[
            {'type': 'inactivity', 'time': '12 hr', 'clan': 'clan_tag'}
        ]
    )

    # Mock clan details
    mock_config = MagicMock()
    mock_clan = MagicMock()
    mock_clan.members = [MagicMock(tag='member_1'), MagicMock(tag='member_2')]
    mock_config.coc_client.get_clan = AsyncMock(return_value=mock_clan)

    # Mock Kafka producer
    mock_kafka_producer = AsyncMock()

    # Initialize tracker
    tracker = InactivityReminderTracker(
        config=mock_config,
        db_client=mock_db_client,
        kafka_producer=mock_kafka_producer,
    )

    # Mock fetch_inactive_members and send_to_kafka
    tracker.fetch_inactive_members = AsyncMock(
        return_value=[
            {'name': 'Alice', 'last_online': 1670000000, '_id': 'member_1'}
        ]
    )
    tracker.send_to_kafka = AsyncMock()

    # Run the tracker
    await tracker.run()

    # Assertions
    tracker.send_to_kafka.assert_awaited_once_with(
        [{'name': 'Alice', 'last_online': 1670000000, '_id': 'member_1'}],
        {'type': 'inactivity', 'time': '12 hr', 'clan': 'clan_tag'},
    )
    mock_config.coc_client.get_clan.assert_awaited_once_with('clan_tag')
    tracker.fetch_inactive_members.assert_awaited_once_with(
        43200, ['member_1', 'member_2']  # 12 hours = 43200 seconds
    )
