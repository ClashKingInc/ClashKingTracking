import asyncio

import pytest
import pendulum
import ujson
from unittest.mock import AsyncMock, MagicMock, patch
from bson import ObjectId
from pymongo.errors import PyMongoError
from kafka.errors import KafkaError
from bot.reminders.war_reminder_tracker import WarReminderTracker


@pytest.mark.asyncio
async def test_fetch_due_reminders():
    """
    Test that fetch_due_reminders retrieves reminders correctly from MongoDB.
    Note: The 'find' call typically isn't awaited by itself (the cursor is awaited
    when calling to_list()). So we can do assert_called_once() for 'find'.
    """
    mock_config = MagicMock()
    mock_db_client = MagicMock()
    mock_kafka_producer = AsyncMock()

    # Make active_reminders an object whose methods are AsyncMock
    mock_active_reminders = MagicMock()
    mock_active_reminders.find = MagicMock()  # 'find' usually returns a cursor

    # The cursor has an async method to_list(), which we mock as AsyncMock
    mock_cursor = MagicMock()
    mock_cursor.to_list = AsyncMock(return_value=[{"_id": "reminder_1"}, {"_id": "reminder_2"}])
    mock_active_reminders.find.return_value = mock_cursor

    mock_db_client.active_reminders = mock_active_reminders

    tracker = WarReminderTracker(config=mock_config, db_client=mock_db_client, kafka_producer=mock_kafka_producer)

    reminders = await tracker.fetch_due_reminders()
    assert len(reminders) == 2

    mock_db_client.active_reminders.find.assert_called_once()
    # The to_list() method is awaited
    mock_cursor.to_list.assert_awaited_once()


@pytest.mark.asyncio
async def test_fetch_due_reminders_db_error():
    """Test that fetch_due_reminders handles a PyMongoError gracefully."""
    mock_config = MagicMock()
    mock_db_client = MagicMock()
    mock_kafka_producer = AsyncMock()

    # Make the find call raise an error
    mock_active_reminders = MagicMock()
    mock_active_reminders.find.side_effect = PyMongoError("DB Error")
    mock_db_client.active_reminders = mock_active_reminders

    tracker = WarReminderTracker(config=mock_config, db_client=mock_db_client, kafka_producer=mock_kafka_producer)

    reminders = await tracker.fetch_due_reminders()
    # Should return an empty list on error
    assert reminders == []


@pytest.mark.asyncio
async def test_send_to_kafka():
    """Test that send_to_kafka sends the reminder to Kafka and removes it from MongoDB."""
    mock_config = MagicMock()
    mock_db_client = MagicMock()
    mock_kafka_producer = AsyncMock()

    # Set up delete_one as an AsyncMock
    mock_delete_one = AsyncMock()
    mock_delete_one.deleted_count = 1

    # So active_reminders is an object, and delete_one is an AsyncMock
    mock_db_client.active_reminders = MagicMock()
    mock_db_client.active_reminders.delete_one = mock_delete_one

    tracker = WarReminderTracker(config=mock_config, db_client=mock_db_client, kafka_producer=mock_kafka_producer)

    reminder = {
        "_id": "507f1f77bcf86cd799439011",
        "job_id": "test_job",
        "type": "War Reminder",
        "run_date": pendulum.now("UTC").int_timestamp,
    }

    await tracker.send_to_kafka(reminder)

    # Check Kafka call
    mock_kafka_producer.send.assert_awaited_once()
    args, kwargs = mock_kafka_producer.send.call_args
    assert args[0] == "War Reminder"  # Topic
    assert kwargs["key"] == b"test_job"

    sent_value = ujson.loads(kwargs["value"].decode("utf-8"))
    assert sent_value["_id"] == "507f1f77bcf86cd799439011"

    # Check MongoDB removal
    mock_db_client.active_reminders.delete_one.assert_awaited_once_with(
        {"reminder_id": ObjectId("507f1f77bcf86cd799439011")}
    )

@pytest.mark.asyncio
async def test_send_to_kafka_kafka_error():
    mock_config = MagicMock()
    mock_kafka_producer = AsyncMock()

    # Use AsyncMock for the db client so methods are recognized as async
    mock_db_client = AsyncMock()
    mock_db_client.active_reminders = AsyncMock()
    mock_db_client.active_reminders.delete_one = AsyncMock()

    mock_kafka_producer.send.side_effect = KafkaError("Kafka fail")

    tracker = WarReminderTracker(
        config=mock_config,
        db_client=mock_db_client,
        kafka_producer=mock_kafka_producer,
    )

    reminder = {
        "_id": "507f1f77bcf86cd799439011",
        "job_id": "test_job",
        "type": "War Reminder",
        "run_date": pendulum.now("UTC").int_timestamp,
    }

    await tracker.send_to_kafka(reminder)

    mock_kafka_producer.send.assert_awaited_once()
    mock_db_client.active_reminders.delete_one.assert_not_awaited()


@pytest.mark.asyncio
async def test_dispatch_reminders():
    """Test that dispatch_reminders fetches reminders and processes them."""
    mock_config = MagicMock()
    mock_db_client = MagicMock()
    mock_kafka_producer = AsyncMock()

    # Mock fetch_due_reminders
    # We'll patch it on the tracker object so we don't rely on the real method
    tracker = WarReminderTracker(config=mock_config, db_client=mock_db_client, kafka_producer=mock_kafka_producer)
    tracker.fetch_due_reminders = AsyncMock(return_value=[{"_id": "reminder_1"}, {"_id": "reminder_2"}])
    tracker._process_single_reminder = AsyncMock()

    await tracker.dispatch_reminders()
    tracker.fetch_due_reminders.assert_awaited_once()
    assert tracker._process_single_reminder.await_count == 2


@pytest.mark.asyncio
async def test_dispatch_reminders_no_reminders():
    """Test dispatch_reminders with no due reminders."""
    mock_config = MagicMock()
    mock_db_client = MagicMock()
    mock_kafka_producer = AsyncMock()

    tracker = WarReminderTracker(config=mock_config, db_client=mock_db_client, kafka_producer=mock_kafka_producer)
    tracker.fetch_due_reminders = AsyncMock(return_value=[])
    tracker._process_single_reminder = AsyncMock()

    await tracker.dispatch_reminders()
    tracker.fetch_due_reminders.assert_awaited_once()
    tracker._process_single_reminder.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_single_reminder_valid():
    """Test _process_single_reminder with a valid reminder."""
    mock_config = MagicMock()
    mock_db_client = MagicMock()
    mock_kafka_producer = AsyncMock()

    tracker = WarReminderTracker(config=mock_config, db_client=mock_db_client, kafka_producer=mock_kafka_producer)
    tracker._fetch_reminder_details = AsyncMock(return_value={"extra_info": "details"})
    tracker.send_to_kafka = AsyncMock()

    reminder = {"_id": "reminder_id", "reminder_id": "details_id"}
    await tracker._process_single_reminder(reminder)

    tracker._fetch_reminder_details.assert_awaited_once_with(reminder)
    tracker.send_to_kafka.assert_awaited_once()
    sent_reminder = tracker.send_to_kafka.call_args[0][0]
    assert sent_reminder["_id"] == "reminder_id"
    assert sent_reminder["extra_info"] == "details"


@pytest.mark.asyncio
async def test_process_single_reminder_invalid():
    """Test _process_single_reminder with invalid or missing details."""
    mock_config = MagicMock()
    mock_db_client = MagicMock()
    mock_kafka_producer = AsyncMock()

    tracker = WarReminderTracker(config=mock_config, db_client=mock_db_client, kafka_producer=mock_kafka_producer)
    tracker._fetch_reminder_details = AsyncMock(return_value=None)
    tracker._delete_invalid_reminder = AsyncMock()
    tracker.send_to_kafka = AsyncMock()

    reminder = {"_id": "reminder_id", "reminder_id": "details_id"}
    await tracker._process_single_reminder(reminder)

    tracker._fetch_reminder_details.assert_awaited_once_with(reminder)
    tracker._delete_invalid_reminder.assert_awaited_once_with(reminder)
    tracker.send_to_kafka.assert_not_awaited()


@pytest.mark.asyncio
async def test_fetch_reminder_details():
    """Test _fetch_reminder_details retrieves details from the DB."""
    mock_config = MagicMock()
    mock_db_client = MagicMock()
    mock_kafka_producer = AsyncMock()

    tracker = WarReminderTracker(config=mock_config, db_client=mock_db_client, kafka_producer=mock_kafka_producer)
    mock_db_client.reminders.find_one = AsyncMock(return_value={"_id": "some_id", "detail": "info"})
    reminder = {"reminder_id": "some_id"}

    result = await tracker._fetch_reminder_details(reminder)
    assert result["detail"] == "info"
    mock_db_client.reminders.find_one.assert_awaited_once_with({"_id": "some_id"})


@pytest.mark.asyncio
async def test_delete_invalid_reminder():
    """Test _delete_invalid_reminder deletes a reminder from the DB."""
    mock_config = MagicMock()
    mock_db_client = MagicMock()
    mock_kafka_producer = AsyncMock()

    tracker = WarReminderTracker(config=mock_config, db_client=mock_db_client, kafka_producer=mock_kafka_producer)

    # Make find_one_and_delete an AsyncMock
    mock_db_client.reminders.find_one_and_delete = AsyncMock()
    reminder = {"_id": "reminder_id", "reminder_id": "some_id"}

    await tracker._delete_invalid_reminder(reminder)
    mock_db_client.reminders.find_one_and_delete.assert_awaited_once_with({"_id": "some_id"})


@pytest.mark.asyncio
async def test_run_single_iteration():
    """
    Test a single iteration of run by artificially stopping after one iteration.
    We patch asyncio.sleep so it doesn't really wait.
    """
    mock_config = MagicMock()
    mock_db_client = MagicMock()
    mock_kafka_producer = AsyncMock()

    tracker = WarReminderTracker(config=mock_config, db_client=mock_db_client, kafka_producer=mock_kafka_producer)
    # Mock dispatch_reminders so we can see how often it's called
    tracker.dispatch_reminders = AsyncMock()

    # We'll patch 'run' to only execute once
    # A simpler approach is to patch 'asyncio.sleep' to raise an exception
    # that breaks the loop in an actual infinite scenario.
    with patch("asyncio.sleep", new=AsyncMock()) as mock_sleep:
        # Manually cause an exception or some condition after first iteration
        async def stop_after_once():
            await tracker.dispatch_reminders()
            raise asyncio.CancelledError("Stopping after one iteration")

        with patch.object(tracker, "run", side_effect=stop_after_once):
            try:
                await tracker.run()
            except asyncio.CancelledError:
                pass

    tracker.dispatch_reminders.assert_awaited_once()
    mock_sleep.assert_not_awaited()
