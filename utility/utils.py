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


def sentry_filter(event, hint):
    """Filter out events that are not errors."""
    if 'exception' in hint:
        exc_type, exc_value, exc_tb = hint['exception']
        if exc_type == KeyboardInterrupt:
            return None
    return event
