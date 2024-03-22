import pendulum as pend
import coc
from datetime import timedelta


def gen_raid_date():
    now = pend.now(tz=pend.UTC)
    current_dayofweek = now.weekday()
    if (current_dayofweek == 4 and now.hour >= 7) or (current_dayofweek == 5) or (current_dayofweek == 6) or (
            current_dayofweek == 0 and now.hour < 7):
        if current_dayofweek == 0:
            current_dayofweek = 7
        fallback = current_dayofweek - 4
        raidDate = (now - timedelta(fallback)).date()
        return str(raidDate)
    else:
        forward = 4 - current_dayofweek
        raidDate = (now + timedelta(forward)).date()
        return str(raidDate)

def gen_season_date():
    end = coc.utils.get_season_end().replace(tzinfo=pend.UTC).date()
    month = end.month
    if month <= 9:
        month = f"0{month}"
    return f"{end.year}-{month}"

def gen_legend_date():
    now = pend.now(tz=pend.UTC)
    hour = now.hour
    if hour < 5:
        date = (now - timedelta(1)).date()
    else:
        date = now.date()
    return str(date)

def gen_games_season():
    now = pend.now(tz=pend.UTC)
    month = now.month
    if month <= 9:
        month = f"0{month}"
    return f"{now.year}-{month}"

def is_raids():
    now = pend.now(tz=pend.UTC)
    current_dayofweek = now.weekday()
    if (current_dayofweek == 4 and now.hour >= 7) or (current_dayofweek == 5) or (current_dayofweek == 6) or (current_dayofweek == 0 and now.hour < 9):
        raid_on = True
    else:
        raid_on = False
    return raid_on







