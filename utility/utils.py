from datetime import timedelta
import coc
import pendulum as pend
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from utility.keycreation import create_keys

def gen_raid_date():
    now = pend.now(tz=pend.UTC)
    current_dayofweek = now.weekday()
    if (
        (current_dayofweek == 4 and now.hour >= 7)
        or (current_dayofweek == 5)
        or (current_dayofweek == 6)
        or (current_dayofweek == 0 and now.hour < 7)
    ):
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
        month = f'0{month}'
    return f'{end.year}-{month}'


def gen_legend_date() -> str:
    now = pend.now(tz=pend.UTC)
    date = now.subtract(days=1).date() if now.hour < 5 else now.date()
    return str(date)


def gen_games_season():
    now = pend.now(tz=pend.UTC)
    month = now.month
    if month <= 9:
        month = f'0{month}'
    return f'{now.year}-{month}'


def is_raids():
    now = pend.now(tz=pend.UTC)
    current_dayofweek = now.weekday()
    if (
        (current_dayofweek == 4 and now.hour >= 7)
        or (current_dayofweek == 5)
        or (current_dayofweek == 6)
        or (current_dayofweek == 0 and now.hour < 9)
    ):
        raid_on = True
    else:
        raid_on = False
    return raid_on

def initialize_scheduler():
    """Initialize the scheduler for periodic tasks."""
    scheduler = AsyncIOScheduler(timezone=pend.UTC)
    scheduler.start()
    return scheduler

async def initialize_coc_client(config):
    """Initialize the coc.py client with the configured keys."""

    keys = await create_keys(
        [
            config.coc_email.format(x=x)
            for x in range(config.min_coc_email, config.max_coc_email + 1)
        ],
        [config.coc_password] * config.max_coc_email,
        as_list=True,
    )
    coc_client = coc.Client(raw_attribute=True, key_count=10, throttle_limit=30)
    await coc_client.login_with_tokens(*keys)
    return coc_client