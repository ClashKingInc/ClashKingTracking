import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from bot.reminders.war_reminder_tracker import WarReminderTracker
from loguru import logger
import pendulum as pend
from bson import ObjectId
import ujson
import asyncio


@pytest.mark.asyncio
async def test_fetch_due_reminders():
    mock_db_client = MagicMock()
    mock_db_client.active_reminders.find.return_value.to_list = AsyncMock(
        return_value=[
            {"job_id": "job_1", "run_date": 1670000000},
            {"job_id": "job_2", "run_date": 1670000010},
        ]
    )

    tracker = WarReminderTracker(config=MagicMock(), db_client=mock_db_client, kafka_producer=AsyncMock())

    reminders = await tracker.fetch_due_reminders()

    assert len(reminders) == 2
    assert reminders[0]["job_id"] == "job_1"
    mock_db_client.active_reminders.find.assert_called_once_with(
        {
            'run_date': {
                '$gte': pend.now(tz=pend.UTC).subtract(minutes=10).int_timestamp,
                '$lt': pend.now(tz=pend.UTC).int_timestamp,
            }
        }
    )


@pytest.mark.asyncio
async def test_send_to_kafka():
    logger.disable("bot.reminders.war_reminder_tracker")
    # Mock Kafka producer and database client
    mock_kafka_producer = AsyncMock()
    mock_db_client = MagicMock()
    mock_db_client.active_reminders.delete_one = AsyncMock()  # Make delete_one an AsyncMock

    # Initialize the tracker
    tracker = WarReminderTracker(config=MagicMock(), db_client=mock_db_client, kafka_producer=mock_kafka_producer)

    # Create a mock reminder
    reminder = {
        "_id": str(ObjectId()),
        "job_id": "job_1",
        "type": "reminder_type",
        "run_date": pend.now(tz=pend.UTC).int_timestamp,
    }

    # Patch ObjectId to ensure consistent behavior
    with patch("bson.ObjectId", return_value=ObjectId()):
        await tracker.send_to_kafka(reminder)

    # Assert Kafka producer send was called with the correct parameters
    mock_kafka_producer.send.assert_awaited_once_with(
        reminder["type"],
        key=reminder["job_id"].encode("utf-8"),
        value=ujson.dumps(reminder).encode("utf-8"),
        timestamp_ms=reminder["run_date"],
    )

    # Assert MongoDB delete_one was called with the correct filter
    mock_db_client.active_reminders.delete_one.assert_awaited_once_with(
        {"reminder_id": ObjectId(reminder["_id"])}
    )


@pytest.mark.asyncio
async def test_dispatch_reminders():
    # Mock the database client and Kafka producer
    mock_db_client = MagicMock()
    mock_kafka_producer = AsyncMock()

    # Mock the reminders fetched from the database
    reminders_mock = AsyncMock()
    reminders_mock.to_list = AsyncMock(
        return_value=[
            {"job_id": "job_1", "run_date": 1670000000},
            {"job_id": "job_2", "run_date": 1670000010},
        ]
    )
    mock_db_client.active_reminders.find.return_value = reminders_mock

    # Initialize the tracker and mock internal methods
    tracker = WarReminderTracker(
        config=MagicMock(), db_client=mock_db_client, kafka_producer=mock_kafka_producer
    )
    tracker._process_single_reminder = AsyncMock()

    # Call the method
    await tracker.dispatch_reminders()

    # Assert the internal method calls
    tracker._process_single_reminder.assert_awaited()
    reminders_mock.to_list.assert_awaited_once()  # Verify to_list was awaited
    mock_db_client.active_reminders.find.assert_called_once()  # Ensure find was called


@pytest.mark.asyncio
async def test_process_single_reminder():
    # Mock the database client and Kafka producer
    mock_db_client = MagicMock()
    mock_kafka_producer = AsyncMock()

    # Create a fixed ObjectId for consistency
    fixed_object_id = ObjectId()

    # Initialize the tracker and mock internal methods
    tracker = WarReminderTracker(config=MagicMock(), db_client=mock_db_client, kafka_producer=mock_kafka_producer)
    tracker._fetch_reminder_details = AsyncMock(return_value={"extra": "details"})
    tracker.send_to_kafka = AsyncMock()

    # Define a reminder with the fixed ObjectId
    reminder = {"job_id": "job_1", "reminder_id": str(fixed_object_id)}

    # Call the method
    await tracker._process_single_reminder(reminder)

    # Assert the fetch_reminder_details method was awaited
    tracker._fetch_reminder_details.assert_awaited_once_with(reminder)

    # Assert the send_to_kafka method was called with the expected enriched reminder
    tracker.send_to_kafka.assert_awaited_once_with(
        {"job_id": "job_1", "reminder_id": str(fixed_object_id), "extra": "details"}
    )


@pytest.mark.asyncio
async def test_run():
    logger.disable("bot.reminders.war_reminder_tracker")
    # Mock the database client and Kafka producer
    mock_db_client = MagicMock()
    mock_kafka_producer = AsyncMock()

    # Initialize the tracker and mock dispatch_reminders method
    tracker = WarReminderTracker(config=MagicMock(), db_client=mock_db_client, kafka_producer=mock_kafka_producer)
    tracker.dispatch_reminders = AsyncMock()

    # Patch asyncio.sleep to prevent real delays
    with patch("asyncio.sleep", new=AsyncMock()) as mock_sleep:
        # Set the mock to stop after the first call
        mock_sleep.side_effect = asyncio.CancelledError()

        # Run the tracker in an asyncio task
        task = asyncio.create_task(tracker.run())

        # Catch the task cancellation to avoid breaking the test
        with pytest.raises(asyncio.CancelledError):
            await task

    # Assert that dispatch_reminders was awaited at least once
    tracker.dispatch_reminders.assert_awaited_once()
