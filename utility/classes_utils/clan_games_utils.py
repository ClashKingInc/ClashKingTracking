import pendulum as pend


def is_clan_games():
    """
    Check if the current time is within the clan games tracking window (22nd 7:00 UTC to 28th 9:00 UTC).
    """
    # Calculate the start and end times for clan games
    now = pend.now('UTC')
    start_time = now.replace(
        day=22, hour=7, minute=0, second=0
    )  # 22nd 7:00 UTC
    end_time = now.replace(day=28, hour=9, minute=0, second=0)  # 28th 9:00 UTC
    # Check if now is within the range
    is_within = start_time <= now < end_time
    return is_within


def get_time_until_next_clan_games():
    """Calculate the time until the next clan games window starts."""
    now = pend.now(tz=pend.UTC)
    try:
        # Fix the day to 22 and hour to 7
        next_clan_games_start = now.start_of('month').set(
            day=22, hour=7, minute=0, second=0
        )
    except ValueError:
        # If the 22nd doesn't exist (e.g., February in a leap year), move to the next month
        next_clan_games_start = (
            now.add(months=1)
            .start_of('month')
            .set(day=22, hour=7, minute=0, second=0)
        )

    if now >= next_clan_games_start:
        # If we're past this month's 22nd 7:00 UTC, move to the next month's 22nd
        next_clan_games_start = next_clan_games_start.add(months=1)

    # Calculate the sleep time
    sleep_time = (next_clan_games_start - now).total_seconds()
    return sleep_time
