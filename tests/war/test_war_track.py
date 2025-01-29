from unittest.mock import AsyncMock, MagicMock

import coc
import pytest

from bot.war.track import WAR_CACHE, WarTracker
from utility.config import TrackingType


@pytest.fixture
def mock_coc_client():
    """Fixture to mock the Clash of Clans client."""
    mock_client = AsyncMock(spec=coc.Client)
    return mock_client


@pytest.fixture
def mock_db_client():
    """Fixture to mock the database client."""
    mock_client = MagicMock()
    return mock_client


@pytest.fixture
def war_tracker(mock_coc_client, mock_db_client):
    """Fixture to initialize the WarTracker class."""
    return WarTracker(
        tracker_type=TrackingType.BOT_WAR, max_concurrent_requests=1000
    )


@pytest.mark.asyncio
async def test_fetch_current_war_private_log(war_tracker, mock_coc_client):
    """Test fetching a current war with a private war log."""
    # Simulate a private war log
    mock_coc_client.get_current_war.side_effect = coc.errors.PrivateWarLog()
    war_tracker.coc_client = mock_coc_client

    result = await war_tracker._fetch_current_war(clan_tag='#12345')
    assert result is None
    mock_coc_client.get_current_war.assert_called_once_with(clan_tag='#12345')


@pytest.mark.asyncio
async def test_process_war_data_first_iteration(war_tracker):
    """Test processing war data during the first iteration."""
    mock_war = MagicMock()
    # Configure mock data
    mock_war.is_cwl = False
    mock_war.clan.tag = '#12345'
    mock_war.opponent.tag = '#67890'
    mock_war.preparation_start_time = MagicMock()
    mock_war.preparation_start_time.time.timestamp.return_value = 1234567890

    war_tracker.is_first_iteration = True

    await war_tracker._process_war_data(clan_tag='#12345', war=mock_war)

    # Verify the war was added to the cache
    assert WAR_CACHE.get('#12345') == mock_war


@pytest.mark.asyncio
async def test_process_war_data_changes_detected_minimal(war_tracker):
    """Minimal test to check state change."""
    # Mock current and previous wars
    mock_current_war = MagicMock(
        state='inWar',
        _raw_data={'state': 'inWar'},
        is_cwl=False,  # Ensure it is not CWL for simplicity
    )
    mock_current_war.clan.tag = '#12345'  # Define as valid string
    mock_current_war.opponent.tag = '#67890'  # Define as valid string
    mock_current_war.preparation_start_time = MagicMock()
    mock_current_war.preparation_start_time.time.timestamp.return_value = (
        1234567890
    )

    mock_previous_war = MagicMock(
        state='preparation',
        _raw_data={'state': 'preparation'},
        is_cwl=False,  # Ensure it is not CWL for simplicity
    )
    mock_previous_war.clan.tag = '#12345'  # Define as valid string

    # Add previous war to the cache
    war_tracker.war_cache['#12345'] = mock_previous_war

    # Mock _send_to_kafka
    war_tracker._send_to_kafka = MagicMock()

    # Ensure is_first_iteration is False
    war_tracker.is_first_iteration = False

    # Call the method
    await war_tracker._process_war_data(
        clan_tag='#12345', war=mock_current_war
    )

    # Assert Kafka messages are sent correctly
    war_tracker._send_to_kafka.assert_called_with(
        'war',
        '#12345',
        {
            'type': 'war_state_change',
            'clan_tag': '#12345',
            'old_state': 'preparation',
            'new_state': 'inWar',
        },
    )


@pytest.mark.asyncio
async def test_fetch_opponent_war_no_result(war_tracker, mock_db_client):
    """Test fetching an opponent's war when no results are found."""
    war_tracker.db_client = mock_db_client
    mock_db_client.clan_wars.find.return_value.sort.return_value.to_list.return_value = (
        []
    )

    result = await war_tracker._fetch_opponent_war(clan_tag='#12345')
    assert result is None
    mock_db_client.clan_wars.find.assert_called_once()


@pytest.mark.asyncio
async def test_is_valid_war(war_tracker):
    """Test validating war data."""
    mock_war = MagicMock()
    mock_war.state = 'inWar'
    mock_war.preparation_start_time = MagicMock()
    mock_war.end_time = MagicMock()

    result = war_tracker._is_valid_war(mock_war, '#12345')
    assert result is True

    # Test invalid war case
    mock_war.state = 'notInWar'
    result = war_tracker._is_valid_war(mock_war, '#12345')
    assert result is False


@pytest.mark.asyncio
async def test_process_cwl_changes(war_tracker):
    """Test processing CWL changes."""
    mock_war = MagicMock()
    mock_previous_war = MagicMock()

    # Mock clan members with proper attributes
    mock_member_1 = MagicMock()
    mock_member_1.tag = '#12345'
    mock_member_1._raw_data = {'tag': '#12345'}

    mock_member_2 = MagicMock()
    mock_member_2.tag = '#67890'
    mock_member_2._raw_data = {'tag': '#67890'}

    mock_previous_member = MagicMock()
    mock_previous_member.tag = '#67890'
    mock_previous_member._raw_data = {'tag': '#67890'}

    # Set up mock data
    mock_war.clan.members = [mock_member_1, mock_member_2]
    mock_previous_war.clan.members = [mock_previous_member]

    # Mock Kafka sender
    war_tracker._send_to_kafka = MagicMock()

    # Call the method
    await war_tracker._process_cwl_changes(mock_war, mock_previous_war)

    # Assert Kafka messages are sent correctly
    war_tracker._send_to_kafka.assert_called_with(
        'cwl_changes',
        mock_war.clan.tag,
        {
            'type': 'cwl_lineup_change',
            'added': [{'tag': '#12345'}],  # Member added
            'removed': [],  # Member removed
            'war': mock_war._raw_data,
        },
    )
