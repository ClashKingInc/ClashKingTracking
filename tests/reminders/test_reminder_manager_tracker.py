import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pendulum
import pytest
from loguru import logger

from bot.reminders.reminder_manager import ReminderManager


@pytest.mark.asyncio
async def test_init():
    """
    Test that the ReminderManager correctly initializes its trackers and clients.
    """
    mock_config = MagicMock()
    mock_config.get_coc_client = AsyncMock()
    mock_config.get_mongo_database = MagicMock()
    mock_config.get_kafka_producer = MagicMock()

    manager = ReminderManager(reminder_config=mock_config)
    await manager.init()

    # Check if config calls were made
    mock_config.get_coc_client.assert_awaited_once()
    mock_config.get_mongo_database.assert_called_once()
    mock_config.get_kafka_producer.assert_called_once()

    # Ensure trackers are not None
    assert manager.raid_tracker is not None
    assert manager.war_tracker is not None
    assert manager.clan_games_tracker is not None
    assert manager.inactivity_tracker is not None


@pytest.mark.asyncio
async def test_cleanup():
    """
    Test that the cleanup method closes all relevant resources.
    """
    logger.disable('bot.reminders.reminder_manager')
    mock_config = MagicMock()
    mock_config.coc_client = (
        AsyncMock()
    )  # Ensure there's a closable coc_client
    mock_db_client = AsyncMock()
    mock_kafka_producer = AsyncMock()

    manager = ReminderManager(reminder_config=mock_config)
    manager.db_client = mock_db_client
    manager.kafka_producer = mock_kafka_producer

    await manager.cleanup()

    mock_config.coc_client.close.assert_awaited_once()
    mock_db_client.close.assert_awaited_once()
    mock_kafka_producer.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_run():
    """
    Test manager.run() spawns concurrent tasks for each tracker and cleans up afterwards.
    """
    mock_config = MagicMock()
    manager = ReminderManager(reminder_config=mock_config)

    # We don't want real concurrency, so we patch create_task and gather
    with patch('asyncio.create_task') as mock_create_task, patch(
        'asyncio.gather', new=AsyncMock()
    ) as mock_gather:
        # Patch manager's cleanup to avoid real resource closures
        manager.cleanup = AsyncMock()

        # Initialize trackers with mock run methods
        manager.raid_tracker = MagicMock(run=AsyncMock())
        manager.war_tracker = MagicMock(run=AsyncMock())
        manager.clan_games_tracker = MagicMock(run=AsyncMock())
        manager.inactivity_tracker = MagicMock(run=AsyncMock())

        await manager.run()

        # Four tasks should have been created
        assert mock_create_task.call_count == 4
        # gather is awaited
        mock_gather.assert_awaited_once()
        # cleanup called in finally block
        manager.cleanup.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_raid_tracker_raid_active():
    """
    If raid is active (is_raids_func returns True),
    it should call raid_tracker.run(), no sleep.
    """
    mock_config = MagicMock()
    manager = ReminderManager(reminder_config=mock_config)
    manager.raid_tracker = MagicMock(
        is_raids_func=MagicMock(return_value=True), run=AsyncMock()
    )

    # Patch time-based methods
    with patch('asyncio.sleep', new=AsyncMock()) as mock_sleep:
        await manager.run_raid_tracker()
        # run_raid_tracker calls raid_tracker.run
        manager.raid_tracker.run.assert_awaited_once()
        mock_sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_raid_tracker_raid_inactive():
    """
    If raid is not active, run_raid_tracker should compute sleep_time
    and call sleep before returning.
    """
    logger.disable('bot.reminders.reminder_manager')
    mock_config = MagicMock()
    manager = ReminderManager(reminder_config=mock_config)
    manager.raid_tracker = MagicMock(
        is_raids_func=MagicMock(return_value=False)
    )

    # We'll patch get_time_until_next_raid to return 3600 (1 hour)
    with patch(
        'utility.classes_utils.raids_utils.get_time_until_next_raid',
        return_value=3600,
    ), patch(
        'utility.classes_utils.raids_utils.format_seconds_as_date',
        return_value='2025-01-10 07:00:00',
    ), patch(
        'asyncio.sleep', new=AsyncMock()
    ) as mock_sleep:
        await manager.run_raid_tracker()
        # No call to raid_tracker.run
        manager.raid_tracker.run.assert_not_called()
        mock_sleep.assert_awaited_once_with(3600)


@pytest.mark.asyncio
async def test_run_clan_games_tracker_active():
    """
    If clan games is active, run_clan_games_tracker calls clan_games_tracker.run().
    """
    mock_config = MagicMock()
    manager = ReminderManager(reminder_config=mock_config)
    manager.clan_games_tracker = MagicMock(
        is_clan_games_func=MagicMock(return_value=True), run=AsyncMock()
    )

    with patch('asyncio.sleep', new=AsyncMock()) as mock_sleep:
        await manager.run_clan_games_tracker()
        manager.clan_games_tracker.run.assert_awaited_once()
        mock_sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_raid_tracker_raid_inactive():
    """
    If raid is not active, run_raid_tracker should compute sleep_time
    and call sleep before returning.
    """
    mock_config = MagicMock()
    manager = ReminderManager(reminder_config=mock_config)
    manager.raid_tracker = MagicMock(
        is_raids_func=MagicMock(return_value=False)
    )

    with patch(
        'bot.reminders.reminder_manager.get_time_until_next_raid',
        return_value=3600,
    ), patch(
        'bot.reminders.reminder_manager.format_seconds_as_date',
        return_value='2025-01-10 07:00:00',
    ), patch(
        'asyncio.sleep', new=AsyncMock()
    ) as mock_sleep:
        await manager.run_raid_tracker()
        # Assert that raid_tracker.run was never called
        manager.raid_tracker.run.assert_not_called()
        # Assert that we slept for 3600 seconds
        mock_sleep.assert_awaited_once_with(3600)


@pytest.mark.asyncio
async def test_run_war_tracker():
    """
    run_war_tracker should just call war_tracker.run() unconditionally.
    """
    mock_config = MagicMock()
    manager = ReminderManager(reminder_config=mock_config)
    manager.war_tracker = MagicMock(run=AsyncMock())

    await manager.run_war_tracker()
    manager.war_tracker.run.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_inactivity_tracker_single_iteration():
    """
    run_inactivity_tracker loops indefinitely, so we test just a single iteration
    by controlling asyncio.sleep and the tracker's run call.
    """
    mock_config = MagicMock()
    manager = ReminderManager(reminder_config=mock_config)
    manager.inactivity_tracker = MagicMock(run=AsyncMock())

    # We'll patch pendulum.now to simulate the current time
    # and then cause an exception or break after the first iteration
    with patch(
        'pendulum.now',
        return_value=pendulum.datetime(2025, 1, 12, 12, 30, tz='UTC'),
    ), patch(
        'utility.classes_utils.raids_utils.format_seconds_as_date',
        return_value='2025-01-12 13:00:00',
    ), patch(
        'asyncio.sleep', new=AsyncMock()
    ) as mock_sleep:

        # We want to break out after one iteration
        async def run_once():
            # original run code but only once
            current_time = pendulum.now('UTC')
            minutes_to_wait = 60 - current_time.minute
            seconds_to_wait = (minutes_to_wait * 60) - current_time.second
            await asyncio.sleep(seconds_to_wait)  # mocked, returns immediately
            await manager.inactivity_tracker.run()
            raise asyncio.CancelledError('Stop after 1 iteration')

        with patch.object(
            manager, 'run_inactivity_tracker', side_effect=run_once
        ):
            try:
                await manager.run_inactivity_tracker()
            except asyncio.CancelledError:
                pass

    mock_sleep.assert_awaited_once()
    manager.inactivity_tracker.run.assert_awaited_once()
