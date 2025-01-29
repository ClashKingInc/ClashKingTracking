import json
from unittest.mock import AsyncMock, MagicMock, patch

import pendulum as pend
import pytest
from loguru import logger

from bot.reminders.clan_games_reminder_tracker import ClanGamesReminderTracker
from utility.utils import serialize


@pytest.mark.asyncio
async def test_fetch_missing_members():
    mock_config = MagicMock()
    mock_db_client = MagicMock()
    mock_kafka_producer = AsyncMock()

    # Mock clan members
    mock_clan_members = [
        MagicMock(
            tag='#80GGY0QLC', name='Alice', town_hall=13, role='coLeader'
        ),
        MagicMock(tag='#YLVPJLPY', name='Bob', town_hall=11, role='leader'),
    ]

    mock_clan_members[0].name = 'Alice'
    mock_clan_members[1].name = 'Bob'

    # Mock MongoDB query
    current_month = pend.now(tz=pend.UTC).start_of('month').format('YYYY-MM')

    mock_db_client.player_stats.find.return_value.to_list = AsyncMock(
        return_value=[
            {
                'tag': '#80GGY0QLC',
                'clan_games': {
                    current_month: {
                        'clan': '#CLANTAG',
                        'points': 100,
                    }
                },
            }
        ]
    )

    # Mock coc_client.get_clan
    mock_config.coc_client.get_clan = AsyncMock(return_value=MagicMock())

    tracker = ClanGamesReminderTracker(
        config=mock_config,
        db_client=mock_db_client,
        kafka_producer=mock_kafka_producer,
    )

    result = await tracker.fetch_missing_members(
        '#CLANTAG',
        mock_clan_members,
        point_threshold=1500,
        townhall_levels=['10', '13'],
        roles=['leader', 'coLeader'],
    )

    # Validate the results
    assert result == {
        '#80GGY0QLC': {
            'name': 'Alice',
            'town_hall': 13,
            'role': 'coLeader',
            'points': 100,
        }
    }


@pytest.mark.asyncio
async def test_fetch_missing_members_no_clan():
    mock_config = MagicMock()
    mock_db_client = MagicMock()
    mock_kafka_producer = AsyncMock()

    mock_config.coc_client.get_clan = AsyncMock(return_value=None)

    tracker = ClanGamesReminderTracker(
        config=mock_config,
        db_client=mock_db_client,
        kafka_producer=mock_kafka_producer,
    )

    result = await tracker.fetch_missing_members(
        '#INVALID_CLAN',
        [],
        point_threshold=1500,
    )

    assert result == {}


@pytest.mark.asyncio
async def test_send_to_kafka():
    mock_kafka_producer = AsyncMock()

    tracker = ClanGamesReminderTracker(
        config=MagicMock(),
        db_client=MagicMock(),
        kafka_producer=mock_kafka_producer,
    )

    reminder = {'type': 'Clan Games', 'clan': '#CLANTAG'}
    members = {'member_1': {'name': 'Alice', 'points': 1000}}

    await tracker.send_to_kafka(reminder, members)
    mock_kafka_producer.send.assert_awaited_once()

    args, kwargs = mock_kafka_producer.send.call_args
    assert args[0] == 'Clan Games'
    assert kwargs['key'] == b'#CLANTAG'

    payload = json.loads(kwargs['value'].decode('utf-8'))
    assert payload == {
        'reminder': json.loads(json.dumps(reminder, default=serialize)),
        'members': json.loads(json.dumps(members, default=serialize)),
    }


@pytest.mark.asyncio
async def test_track_clan_games_reminders():
    logger.disable('bot.reminders.clan_games_reminder_tracker')
    mock_config = MagicMock()
    mock_db_client = MagicMock()
    mock_kafka_producer = AsyncMock()

    # Mock MongoDB query
    mock_db_client.reminders.find.return_value.to_list = AsyncMock(
        return_value=[
            {
                'type': 'Clan Games',
                'time': '24 hr',
                'clan': '#CLANTAG',
                'point_threshold': 1500,
            }
        ]
    )

    tracker = ClanGamesReminderTracker(
        config=mock_config,
        db_client=mock_db_client,
        kafka_producer=mock_kafka_producer,
    )

    tracker.process_reminder = AsyncMock()

    await tracker.track_clan_games_reminders()

    tracker.process_reminder.assert_awaited_once()
    mock_db_client.reminders.find.return_value.to_list.assert_awaited_once()  # Correct the assertion


@pytest.mark.asyncio
async def test_run_clan_games_loop():
    logger.disable('bot.reminders.clan_games_reminder_tracker')
    tracker = ClanGamesReminderTracker(
        config=MagicMock(), db_client=MagicMock(), kafka_producer=AsyncMock()
    )
    tracker.track_clan_games_reminders = AsyncMock()

    # Let's say we want 2 iterations before returning False so that the loop ends.
    loop_side_effects = [True, True, False]
    tracker.is_clan_games_func = MagicMock(side_effect=loop_side_effects)

    # Patch asyncio.sleep so it doesn't really sleep
    with patch('asyncio.sleep', new=AsyncMock()) as mock_sleep:
        # Patch pendulum.now for each iteration, if desired
        # For 2 iterations + final check, we need 3 calls
        mock_now_values = [
            # First iteration
            pend.datetime(2025, 1, 22, 7, 0, tz='UTC'),  # start_time
            pend.datetime(2025, 1, 22, 7, 30, tz='UTC'),  # now
            pend.datetime(
                2025, 1, 22, 7, 30, tz='UTC'
            ),  # end_time of first iteration
            # Second iteration
            pend.datetime(2025, 1, 22, 8, 0, tz='UTC'),  # start_time
            pend.datetime(2025, 1, 22, 8, 30, tz='UTC'),  # now
            pend.datetime(
                2025, 1, 22, 8, 30, tz='UTC'
            ),  # end_time of second iteration
        ]
        with patch('pendulum.now', side_effect=mock_now_values):
            await tracker.run()

    # Check that track_clan_games_reminders was called exactly 2 times
    assert tracker.track_clan_games_reminders.await_count == 2

    # is_clan_games_func should have been called 3 times: True, True, then False
    assert tracker.is_clan_games_func.call_count == 3

    # asyncio.sleep should have been awaited 2 times (one for each loop iteration)
    assert mock_sleep.await_count == 2
