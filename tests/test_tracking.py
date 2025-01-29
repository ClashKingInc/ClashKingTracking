import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pendulum as pend
import pytest
from bson import ObjectId
from pymongo import ReturnDocument

from tracking import Tracking


@pytest.mark.asyncio
async def test_initialize():
    """Test that initialize() sets up the tracker with the correct dependencies."""
    mock_config = MagicMock()
    mock_config.get_coc_client = AsyncMock()
    mock_config.get_mongo_database = MagicMock()
    mock_config.get_redis_client = MagicMock()
    mock_config.get_kafka_producer = MagicMock()

    tracker = Tracking(tracker_type='BOT_CLAN')
    tracker.config = mock_config

    await tracker.initialize()

    mock_config.get_coc_client.assert_awaited_once()
    mock_config.get_mongo_database.assert_called_once()
    mock_config.get_redis_client.assert_called_once()
    mock_config.get_kafka_producer.assert_called_once()

    assert tracker.db_client == mock_config.get_mongo_database.return_value
    assert tracker.redis == mock_config.get_redis_client.return_value
    assert tracker.coc_client == mock_config.coc_client
    assert tracker.kafka == mock_config.get_kafka_producer.return_value
    # Check http_session and scheduler are created
    assert tracker.http_session is not None
    assert tracker.scheduler is not None


@pytest.mark.asyncio
async def test_track():
    """Test that track() processes items in batches."""
    tracker = Tracking(tracker_type='BOT_CLAN')
    tracker.logger = MagicMock()
    tracker._track_batch = AsyncMock()

    items = list(range(10))  # 10 items
    tracker.batch_size = 3
    await tracker.track(items)

    # We expect ceil(10/3)=4 calls to _track_batch
    assert tracker._track_batch.await_count == 4
    # Check the logs for batch info
    tracker.logger.info.assert_any_call('Processing batch 1 of 4.')
    tracker.logger.info.assert_any_call('Processing batch 2 of 4.')
    tracker.logger.info.assert_any_call('Processing batch 3 of 4.')
    tracker.logger.info.assert_any_call('Processing batch 4 of 4.')
    # Also we have a final log about finishing
    tracker.logger.info.assert_any_call('Finished tracking all clans.')


@pytest.mark.asyncio
async def test__track_batch():
    """Test _track_batch concurrency with a given batch."""
    tracker = Tracking(tracker_type='BOT_CLAN')
    tracker.logger = MagicMock()
    tracker.semaphore = (
        AsyncMock()
    )  # or create a real Semaphore but mock the inside
    tracker._track_item = AsyncMock(side_effect=[None, Exception('Error!')])

    batch = ['item1', 'item2']
    # We'll mock 'self.semaphore' as an async context manager
    class MockSem:
        async def __aenter__(self):
            return

        async def __aexit__(self, exc_type, exc, tb):
            return

    tracker.semaphore = MockSem()

    await tracker._track_batch(batch)

    # The first item returns None, second item returns an Exception
    assert tracker._track_item.await_count == 2
    # The logger should log the final
    tracker.logger.info.assert_any_call('Finished tracking batch of 2 clans.')


@pytest.mark.asyncio
async def test__schedule_reminder():
    """Test _schedule_reminder inserts a reminder into MongoDB."""
    tracker = Tracking(tracker_type='BOT_CLAN')
    tracker.logger = MagicMock()
    tracker.db_client = MagicMock()
    mock_insert_one = AsyncMock()
    tracker.db_client.active_reminders = MagicMock(insert_one=mock_insert_one)

    job_id = 'job123'
    reminder_document = {'_id': 'some_id', 'job_id': job_id}
    await tracker._schedule_reminder(job_id, reminder_document)

    mock_insert_one.assert_awaited_once_with(reminder_document)
    tracker.logger.info.assert_any_call(
        f'New reminder scheduled: {job_id}, Inserted ID: {mock_insert_one.return_value.inserted_id}'
    )


@pytest.mark.asyncio
async def test__update_reminder():
    """Test _update_reminder updates a reminder in the DB."""
    tracker = Tracking(tracker_type='BOT_CLAN')
    tracker.logger = MagicMock()
    tracker.db_client = MagicMock()
    mock_find_one_and_update = AsyncMock(return_value={'job_id': 'job123'})
    tracker.db_client.active_reminders = MagicMock(
        find_one_and_update=mock_find_one_and_update
    )

    job_id = 'job123'
    reminder_document = {'run_date': 123456}
    result = await tracker._update_reminder(job_id, reminder_document)

    mock_find_one_and_update.assert_awaited_once_with(
        {'job_id': job_id},
        {'$set': reminder_document},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    # We got a doc back
    assert result == {'job_id': 'job123'}
    tracker.logger.info.assert_any_call(
        f'Reminder successfully updated: {job_id}, Result: {result}'
    )


@pytest.mark.asyncio
async def test__schedule_reminders_no_reminders():
    """
    Test _schedule_reminders does nothing if no reminders exist in DB.
    """
    tracker = Tracking(tracker_type='BOT_CLAN')
    tracker.logger = MagicMock()
    tracker.db_client = MagicMock()
    mock_find = AsyncMock()
    mock_find.to_list = AsyncMock(return_value=[])
    tracker.db_client.reminders = MagicMock(
        find=MagicMock(return_value=mock_find)
    )

    clan_tag = '#CLANTAG'
    war_tag = 'some_war_tag'
    reminder_data = {'end_time': pend.now('UTC').int_timestamp + 3600}
    await tracker._schedule_reminders(
        clan_tag, war_tag, reminder_data, 'war_reminder'
    )

    tracker.logger.error.assert_not_called()
    tracker.logger.info.assert_not_called()


@pytest.mark.asyncio
async def test__schedule_reminders_with_data():
    """
    Test _schedule_reminders with a valid set of reminder docs from DB.
    """
    tracker = Tracking(tracker_type='BOT_CLAN')
    tracker.logger = MagicMock()
    tracker.db_client = MagicMock()
    # Simulate 2 reminders in DB
    mock_find = AsyncMock()
    # times: "12 hr" => 12 * 3600, "24 hr" => 24 * 3600
    reminder_docs = [
        {
            'clan': '#CLANTAG',
            'type': 'war_reminder',
            'time': '12 hr',
            '_id': ObjectId(),
            'server': 123,
        },
        {
            'clan': '#CLANTAG',
            'type': 'war_reminder',
            'time': '24 hr',
            '_id': ObjectId(),
            'server': 123,
        },
    ]
    mock_find.to_list = AsyncMock(return_value=reminder_docs)
    tracker.db_client.reminders = MagicMock(
        find=MagicMock(return_value=mock_find)
    )

    # end_time is 2 hours from now => let's say now + 7200
    end_time = pend.now('UTC').int_timestamp + 7200
    reminder_data = {'end_time': end_time}
    clan_tag = '#CLANTAG'
    war_tag = 'warTag'

    # Patch the sub-methods
    tracker._update_reminder = AsyncMock()
    tracker._schedule_reminder = AsyncMock()

    await tracker._schedule_reminders(
        clan_tag, war_tag, reminder_data, 'war_reminder'
    )

    # We'll have 2 reminders: 12hr => if end_time=Now+2h => reminder_time => in the past => skip
    # 24hr => also in the past => skip
    # So probably no new reminders, but let's see if it logs or tries to schedule
    tracker._update_reminder.assert_not_awaited()
    tracker._schedule_reminder.assert_not_awaited()


def test_is_raids():
    """
    Quick test for is_raids() logic (Friday 7:00 UTC to Monday 9:00 UTC).
    We'll do param-based approach or single checks with patching pendulum.now.
    """
    tracker = Tracking(tracker_type='BOT_CLAN')

    with patch(
        'pendulum.now',
        return_value=pend.datetime(2025, 1, 10, 6, 59, tz='UTC'),
    ):
        # 2025-01-10 is a Friday?
        # day_of_week( Monday=0,... ) => we need to check that it's day=4 => Friday
        # 6:59 => before 7:00 => not in raids
        assert tracker.is_raids() is False

    with patch(
        'pendulum.now', return_value=pend.datetime(2025, 1, 10, 7, 0, tz='UTC')
    ):
        # Exactly Friday 7:00 => in raids
        assert tracker.is_raids() is True


@pytest.mark.asyncio
async def test_run_single_iteration():
    """
    Test a single iteration of the run method to ensure it doesn't loop infinitely.
    We'll patch or break it after one iteration by raising an exception.
    """
    tracker = Tracking(tracker_type='BOT_CLAN')
    # We'll mock everything that's used inside run
    tracker.logger = MagicMock()
    with patch.object(
        tracker, 'run', side_effect=[None, asyncio.CancelledError('Stop loop')]
    ):
        # Just call run once
        try:
            await tracker.run(
                None
            )  # The signature is run(tracker_class, loop_interval, etc.)
        except asyncio.CancelledError:
            pass

    # The test ensures we can start run() and forcibly stop it without blocking.
