from unittest.mock import AsyncMock, MagicMock

import coc
import pytest

from bot.raids.track import RaidTracker
from utility.config import TrackingType


@pytest.fixture
def raid_tracker():
    """Fixture to create a RaidTracker instance."""
    tracker = RaidTracker(tracker_type=TrackingType.BOT_RAIDS)
    tracker.coc_client = AsyncMock()
    tracker.db_client = AsyncMock()
    tracker._send_to_kafka = AsyncMock()
    return tracker


@pytest.mark.asyncio
async def test_get_current_raid(raid_tracker):
    """Test fetching the current raid."""
    mock_raid = MagicMock()
    raid_tracker.coc_client.get_raid_log.return_value = [mock_raid]

    result = await raid_tracker._get_current_raid('#12345')

    assert result == mock_raid
    raid_tracker.coc_client.get_raid_log.assert_called_once_with(
        clan_tag='#12345', limit=1
    )


@pytest.mark.asyncio
async def test_get_previous_raid(raid_tracker):
    """Test retrieving the previous raid from the database."""
    mock_data = {'tag': '#12345', 'data': {'mock_key': 'mock_value'}}
    raid_tracker.db_client.capital_cache.find_one.return_value = mock_data

    result = await raid_tracker._get_previous_raid('#12345')

    assert isinstance(result, coc.RaidLogEntry)
    assert result._raw_data == mock_data['data']
    raid_tracker.db_client.capital_cache.find_one.assert_called_once_with(
        {'tag': '#12345'}
    )


@pytest.mark.asyncio
async def test_process_raid_changes_no_changes(raid_tracker):
    """Test that no Kafka event is sent if the raid has not changed."""
    mock_raid = MagicMock()
    mock_raid._raw_data = {'mock_key': 'mock_value'}

    await raid_tracker._process_raid_changes('#12345', mock_raid, mock_raid)

    raid_tracker._send_to_kafka.assert_not_called()


@pytest.mark.asyncio
async def test_process_raid_changes_with_changes(raid_tracker):
    """Test that changes in the raid trigger updates."""
    current_raid = MagicMock()
    current_raid._raw_data = {'mock_key': 'new_value'}
    previous_raid = MagicMock()
    previous_raid._raw_data = {'mock_key': 'old_value'}

    raid_tracker.db_client.capital_cache.update_one = AsyncMock()

    await raid_tracker._process_raid_changes(
        '#12345', current_raid, previous_raid
    )

    raid_tracker.db_client.capital_cache.update_one.assert_called_once()
    raid_tracker._send_to_kafka.assert_not_called()  # Les attaques sont traitées séparément


@pytest.mark.asyncio
async def test_detect_new_opponents(raid_tracker):
    """Test detecting new offensive opponents."""
    mock_current_raid = MagicMock()
    mock_previous_raid = MagicMock()

    mock_opponent = MagicMock()
    mock_opponent._raw_data = {'opponent': 'new_clan'}

    mock_current_raid.attack_log = [mock_opponent]
    mock_previous_raid.attack_log = []

    raid_tracker._check_raids_settings = AsyncMock(
        return_value=[{'server': '1234', 'new_raid_panel_webhook_id': '5678'}]
    )

    await raid_tracker._detect_new_opponents(
        '#12345', mock_current_raid, mock_previous_raid
    )

    raid_tracker._send_to_kafka.assert_called_once_with(
        'capital',
        '#12345',
        {
            'type': 'new_offensive_opponent',
            'server': '1234',
            'webhook_id': '5678',
            'clan': mock_opponent._raw_data,
            'clan_tag': '#12345',
            'previous_raid': mock_previous_raid._raw_data,
        },
    )


@pytest.mark.asyncio
async def test_detect_member_attacks(raid_tracker):
    """Test detecting new member attacks and sending them to Kafka."""
    current_raid = MagicMock()
    previous_raid = MagicMock()

    # Fix the MagicMock issue for clan_tag
    current_raid.clan_tag = '#12345'
    previous_raid.clan_tag = '#12345'

    current_raid._raw_data = {
        'attackLog': [
            {
                'defender': {'tag': '#DISTRICT1', 'name': 'District One'},
                'districts': [
                    {
                        'id': 70000001,
                        'attackCount': 2,
                        'totalLooted': 5000,
                        'attacks': [
                            {
                                'attacker': {
                                    'tag': '#PLAYER1',
                                    'name': 'Attacker1',
                                },
                                'destructionPercent': 50,
                                'stars': 2,
                            }
                        ],
                    }
                ],
            }
        ]
    }

    previous_raid._raw_data = {'attackLog': []}

    raid_tracker._check_raids_settings = AsyncMock(
        return_value=[{'server': '1234', 'capital_attacks_webhook_id': '5678'}]
    )

    await raid_tracker._detect_member_attacks(
        '#12345', current_raid, previous_raid
    )

    raid_tracker._send_to_kafka.assert_called_once_with(
        'capital',
        '#12345',
        {
            'type': 'raid_attacks',
            'clan_tag': '#12345',  # Now it's a string, not a MagicMock
            'server': '1234',
            'webhook_id': '5678',
            'detailed_attacks': [
                {
                    'district_name': 'District One',
                    'district_tag': '#DISTRICT1',
                    'attacker_tag': '#PLAYER1',
                    'attacker_name': 'Attacker1',
                    'stars': 2,
                    'destruction_percentage': 50,
                    'cumulative_destruction_percentage': 50,
                    'total_looted': 5000,
                    'total_attacks_on_district': 2,
                }
            ],
        },
    )
