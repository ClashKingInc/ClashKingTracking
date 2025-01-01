from aiohttp import ClientSession
from collections import deque
import coc
import pendulum as pend
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from utility.keycreation import create_keys


async def fetch(url: str, session: ClientSession, tag: str, keys: deque, throttler):
    async with throttler:
        keys.rotate(1)
        async with session.get(url, headers={'Authorization': f'Bearer {keys[0]}'}) as response:
            if response.status == 200:
                return (await response.read(), tag)
            return (None, None)


def initialize_scheduler():
    """Initialize the scheduler for periodic tasks."""
    scheduler = AsyncIOScheduler(timezone=pend.UTC)
    scheduler.start()
    return scheduler

def is_raid_tracking_time():
    """
    Check if the current time is within the raid tracking window (Friday 7:00 UTC to Monday 7:00 UTC).
    """
    now = pend.now("UTC")
    friday_7am = now.start_of("week").add(days=4, hours=7)
    monday_7am = now.start_of("week").add(days=7, hours=7)
    return friday_7am <= now < monday_7am


def sentry_filter(event, hint):
    """Filter out events that are not errors."""
    if 'exception' in hint:
        exc_type, exc_value, exc_tb = hint['exception']
        if exc_type == KeyboardInterrupt:
            return None
    return event
