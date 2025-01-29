from unittest.mock import AsyncMock, MagicMock

import pytest

from bot.clan.track import ClanTracker
from utility.config import TrackingType


@pytest.fixture
def clan_tracker():
    """Fixture to create an instance of ClanTracker with mock dependencies."""
    tracker = ClanTracker(tracker_type=TrackingType.BOT_CLAN)
    tracker.coc_client = AsyncMock()  # Mock COC client
    tracker._send_to_kafka = AsyncMock()  # Ensure _send_to_kafka is a mock
    tracker._handle_exception = AsyncMock()  # Mock exception handler
    return tracker


@pytest.mark.asyncio
async def test_track_item_fetch_error(clan_tracker):
    """Test error handling when fetching clan details fails."""
    clan_tracker.coc_client.get_clan.side_effect = Exception('Fetch Error')

    await clan_tracker._track_item('#12345')

    clan_tracker._handle_exception.assert_called_once_with(
        'Error fetching clan #12345',
        clan_tracker.coc_client.get_clan.side_effect,
    )


@pytest.mark.asyncio
async def test_track_item_no_previous_clan(clan_tracker):
    """Test that tracking a clan for the first time does not trigger updates."""
    mock_clan = MagicMock(tag='#12345')
    clan_tracker.coc_client.get_clan.return_value = mock_clan

    await clan_tracker._track_item('#12345')

    assert clan_tracker.clan_cache['#12345'] == mock_clan
    clan_tracker._send_to_kafka.assert_not_called()


@pytest.mark.asyncio
async def test_handle_private_warlog_no_warn(clan_tracker):
    """Test that no warning is sent if war log is public."""
    mock_clan = MagicMock(tag='#12345', public_war_log=True)

    await clan_tracker._handle_private_warlog(mock_clan)

    clan_tracker._send_to_kafka.assert_not_called()


@pytest.mark.asyncio
async def test_handle_private_warlog_warn(clan_tracker):
    """Test that a warning is sent when war log is private."""
    mock_clan = MagicMock(tag='#12345', public_war_log=False)

    await clan_tracker._handle_private_warlog(mock_clan)

    clan_tracker._send_to_kafka.assert_called_once_with(
        'clan',
        '#12345',
        {'type': 'war_log_closed', 'clan': mock_clan._raw_data},
    )


@pytest.mark.asyncio
async def test_handle_attribute_changes_no_change(clan_tracker):
    """Test that no event is sent when there are no attribute changes."""
    mock_clan = MagicMock(
        tag='#12345',
        level=10,
        type='inviteOnly',
        description='A clan',
        location='France',
        capital_league='Gold',
        required_townhall=10,
        required_trophies=2000,
        war_win_streak=2,
        war_league='Crystal',
        member_count=50,
        _raw_data={'mock_data': 'unchanged'},
    )

    previous_clan = MagicMock(
        **mock_clan.__dict__
    )  # Clone with identical attributes

    await clan_tracker._handle_attribute_changes(mock_clan, previous_clan)

    clan_tracker._send_to_kafka.assert_not_called()


@pytest.mark.asyncio
async def test_handle_attribute_changes_detected(clan_tracker):
    """Test that attribute changes trigger a Kafka event."""
    mock_clan = MagicMock(
        tag='#12345',
        level=11,  # Level changed
        location='Germany',
        capital_league='Silver',
        required_townhall=11,
        required_trophies=2500,
        war_win_streak=3,
        war_league='Master',
        member_count=49,  # Member count changed
        type='inviteOnly',
        description='A clan',
        _raw_data={'mock_data': 'updated'},
    )

    previous_clan = MagicMock(
        tag='#12345',
        level=10,
        location='Germany',
        capital_league='Silver',
        required_townhall=11,
        required_trophies=2500,
        war_win_streak=3,
        war_league='Master',
        member_count=48,  # Member count changed
        type='inviteOnly',
        description='A clan',
        _raw_data={'mock_data': 'old'},
    )

    await clan_tracker._handle_attribute_changes(mock_clan, previous_clan)

    clan_tracker._send_to_kafka.assert_called_once_with(
        'clan',
        '#12345',
        {
            'types': ['level', 'member_count'],
            'old_clan': previous_clan._raw_data,
            'new_clan': mock_clan._raw_data,
        },
    )


@pytest.mark.asyncio
async def test_handle_member_changes_no_change(clan_tracker):
    """Test that no event is sent when there are no member changes."""
    mock_clan = MagicMock(
        tag='#12345',
        members_dict={
            '#A1': MagicMock(tag='#A1'),
            '#B2': MagicMock(tag='#B2'),
        },
        _raw_data={'mock_data': 'unchanged'},
    )

    previous_clan = MagicMock(
        **mock_clan.__dict__
    )  # Clone with identical members

    await clan_tracker._handle_member_changes(mock_clan, previous_clan)

    clan_tracker._send_to_kafka.assert_not_called()


@pytest.mark.asyncio
async def test_handle_member_changes_detected(clan_tracker):
    """Test that member join/leave events trigger a Kafka event."""

    mock_clan = MagicMock()
    mock_clan.tag = '#12345'
    mock_clan.members_dict = {
        '#A1': MagicMock(tag='#A1'),  # Ancien membre toujours présent
        '#C3': MagicMock(tag='#C3'),  # Nouveau membre (a rejoint)
    }
    mock_clan.members = list(
        mock_clan.members_dict.values()
    )  # ✅ Définir `members`
    mock_clan._raw_data = {'mock_data': 'updated'}

    previous_clan = MagicMock()
    previous_clan.tag = '#12345'
    previous_clan.members_dict = {
        '#A1': MagicMock(tag='#A1'),  # Ancien membre toujours présent
        '#B2': MagicMock(tag='#B2'),  # Membre qui a quitté
    }
    previous_clan.members = list(
        previous_clan.members_dict.values()
    )  # ✅ Définir `members`
    previous_clan._raw_data = {'mock_data': 'old'}

    clan_tracker._send_to_kafka = AsyncMock()

    await clan_tracker._handle_member_changes(mock_clan, previous_clan)

    clan_tracker._send_to_kafka.assert_called_once_with(
        'clan',
        '#12345',
        {
            'type': 'members_join_leave',
            'old_clan': previous_clan._raw_data,
            'new_clan': mock_clan._raw_data,
            'joined': [mock_clan.members_dict['#C3']._raw_data],
            'left': [previous_clan.members_dict['#B2']._raw_data],
        },
    )


@pytest.mark.asyncio
async def test_handle_donation_updates_no_change(clan_tracker):
    """Test that no event is sent when there are no donation updates."""
    mock_clan = MagicMock(
        tag='#12345',
        members=[
            MagicMock(tag='#A1', donations=10, received=5),
            MagicMock(tag='#B2', donations=20, received=10),
        ],
        _raw_data={'mock_data': 'unchanged'},
    )

    previous_clan = MagicMock(
        **mock_clan.__dict__
    )  # Clone with identical donations

    await clan_tracker._handle_donation_updates(mock_clan, previous_clan)

    clan_tracker._send_to_kafka.assert_not_called()


@pytest.mark.asyncio
async def test_handle_donation_updates_detected(clan_tracker):
    """Test that donation updates trigger a Kafka event."""
    mock_clan = MagicMock(
        tag='#12345',
        members=[
            MagicMock(
                tag='#A1', donations=15, received=5
            ),  # Increased donations
            MagicMock(tag='#B2', donations=20, received=10),
        ],
        _raw_data={'mock_data': 'updated'},
    )

    previous_clan = MagicMock(
        tag='#12345',
        members=[
            MagicMock(tag='#A1', donations=10, received=5),
            MagicMock(tag='#B2', donations=20, received=10),
        ],
        _raw_data={'mock_data': 'old'},
    )

    await clan_tracker._handle_donation_updates(mock_clan, previous_clan)

    clan_tracker._send_to_kafka.assert_called_once_with(
        'clan',
        '#12345',
        {
            'type': 'all_member_donations',
            'old_clan': previous_clan._raw_data,
            'new_clan': mock_clan._raw_data,
        },
    )
