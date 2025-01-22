import pytest
import pendulum
from unittest.mock import AsyncMock, MagicMock, patch
from utility.classes_utils.raids_utils import (
    is_raids,
    get_time_until_next_raid,
    format_seconds_as_date,
    weekend_to_coc_py_timestamp,
    get_raid_log_entry,
    calculate_current_weekend,
)


@pytest.mark.parametrize(
    "current_time, expected",
    [
        ("2025-01-10 06:59:59", False),  # Before raid tracking starts
        ("2025-01-10 07:00:00", True),   # Exactly at the start of raid tracking
        ("2025-01-12 12:00:00", True),   # During raid tracking
        ("2025-01-13 09:00:00", False),  # Exactly at the end of raid tracking
        ("2025-01-14 10:00:00", False),  # After raid tracking ends
    ],
)
def test_is_raids(current_time, expected):
    """Test is_raids() for various times to ensure correct raid tracking."""
    with patch("pendulum.now", return_value=pendulum.parse(current_time, tz="UTC")):
        assert is_raids() == expected


@pytest.mark.parametrize(
    "current_time, next_start_time",
    [
        ("2025-01-10 06:59:59", "2025-01-10 07:00:00"),  # Before raid tracking
        ("2025-01-14 10:00:00", "2025-01-17 07:00:00"),  # After raid tracking
    ],
)
def test_get_time_until_next_raid(current_time, next_start_time):
    """Test get_time_until_next_raid() for correct sleep time calculation."""
    with patch("pendulum.now", return_value=pendulum.parse(current_time, tz="UTC")):
        sleep_time = get_time_until_next_raid()
        expected_start = pendulum.parse(next_start_time, tz="UTC")
        current = pendulum.parse(current_time, tz="UTC")
        assert abs((expected_start - current).total_seconds() - sleep_time) < 1


def test_format_seconds_as_date():
    """Test format_seconds_as_date() for correct datetime conversion."""
    now = pendulum.now("UTC")
    seconds = 3600
    expected = now.add(seconds=seconds).to_datetime_string()
    result = format_seconds_as_date(seconds)
    assert result == expected


def test_weekend_to_coc_py_timestamp():
    """Test weekend_to_coc_py_timestamp() to ensure it creates the correct Python datetime."""
    weekend = "2025-01-10"

    # Test the start timestamp
    result_start = weekend_to_coc_py_timestamp(weekend, end=False)
    # Check the resulting datetime fields
    assert result_start.time.year == 2025
    assert result_start.time.month == 1
    assert result_start.time.day == 10
    assert result_start.time.hour == 7
    assert result_start.time.minute == 0

    # Test the end timestamp (3 days after the start)
    result_end = weekend_to_coc_py_timestamp(weekend, end=True)
    assert result_end.time.year == 2025
    assert result_end.time.month == 1
    assert result_end.time.day == 13  # 10 + 3
    assert result_end.time.hour == 7
    assert result_end.time.minute == 0


def test_calculate_current_weekend():
    """Test calculate_current_weekend() to calculate the correct weekend start date."""
    with patch("pendulum.now", return_value=pendulum.parse("2025-01-12 12:00:00", tz="UTC")):
        result = calculate_current_weekend()
        expected = "2025-01-10"
        assert result == expected
