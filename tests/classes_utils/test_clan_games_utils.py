import pytest
import pendulum
from utility.classes_utils.clan_games_utils import get_time_until_next_clan_games, is_clan_games
from unittest.mock import patch

@pytest.mark.parametrize(
    "current_time, expected",
    [
        ("2025-01-21 06:59:59", False),  # Before the clan games start
        ("2025-01-22 07:00:00", True),   # Exactly at the start of the clan games
        ("2025-01-25 12:00:00", True),   # During the clan games
        ("2025-01-28 09:00:00", False),  # Exactly at the end of the clan games
        ("2025-01-29 10:00:00", False),  # After the clan games end
    ],
)
def test_is_clan_games(current_time, expected):
    # Parse the mocked current time
    test_time = pendulum.parse(current_time, tz="UTC")

    # Patch pendulum.now to return the mocked time
    with patch("pendulum.now", return_value=test_time):
        # Call the function and assert the result
        assert is_clan_games() == expected

@pytest.mark.parametrize(
    "current_time, next_start_time",
    [
        # Case: Before the start of the clan games
        ("2025-01-10 10:00:00", "2025-01-22 07:00:00"),
        # Case: During the clan games
        ("2025-01-25 12:00:00", "2025-02-22 07:00:00"),
        # Case: After the clan games
        ("2025-01-29 12:00:00", "2025-02-22 07:00:00"),
        # Case: Spanning across two years
        ("2024-12-30 10:00:00", "2025-01-22 07:00:00"),
    ],
)
def test_get_time_until_next_clan_games(current_time, next_start_time):
    # Parse the mocked current time
    test_time = pendulum.parse(current_time, tz="UTC")

    # Patch pendulum.now to return the mocked time
    with patch("pendulum.now", return_value=test_time):
        # Get the time until the next clan games
        sleep_time = get_time_until_next_clan_games()

        # Parse the expected next start time
        next_start = pendulum.parse(next_start_time, tz="UTC")

        # Assert that the calculated sleep time matches the difference between the current time and the next start time
        assert abs((next_start - test_time).total_seconds() - sleep_time) < 1
