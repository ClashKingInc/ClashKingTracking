import coc
import pendulum as pend


def gen_raid_date():
    now = pend.now(tz=pend.UTC)
    current_dayofweek = now.day_of_week  # Monday = 0, Sunday = 6
    if (
        (current_dayofweek == 4 and now.hour >= 7)  # Friday after 7 AM UTC
        or (current_dayofweek == 5)  # Saturday
        or (current_dayofweek == 6)  # Sunday
        or (current_dayofweek == 0 and now.hour < 7)  # Monday before 7 AM UTC
    ):
        raid_date = now.subtract(days=(current_dayofweek - 4 if current_dayofweek >= 4 else 0)).date()
    else:
        forward = 4 - current_dayofweek  # Days until next Friday
        raid_date = now.add(days=forward).date()
    return str(raid_date)


def gen_season_date():
    end = coc.utils.get_season_end().astimezone(pend.UTC)
    month = f"{end.month:02}"
    return f"{end.year}-{month}"


def gen_legend_date():
    now = pend.now(tz=pend.UTC)
    date = now.subtract(days=1).date() if now.hour < 5 else now.date()
    return str(date)


def gen_games_season():
    now = pend.now(tz=pend.UTC)
    month = f"{now.month:02}"  # Ensure two-digit month
    return f"{now.year}-{month}"


def is_raids():
    """
    Check if the current time is within the raid tracking window (Friday 7:00 UTC to Monday 9:00 UTC).
    """
    now = pend.now("UTC")
    friday_7am = now.start_of("week").add(days=4, hours=7)
    monday_9am = now.start_of("week").add(days=7, hours=9)
    return friday_7am <= now < monday_9am


def is_cwl():
    now = pend.now(tz=pend.UTC)
    return 1 <= now.day <= 10 and not ((now.day == 1 and now.hour < 8) or (now.day == 11 and now.hour >= 8))
