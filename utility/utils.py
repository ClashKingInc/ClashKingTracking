from aiohttp import ClientSession
from collections import deque


async def fetch(url: str, session: ClientSession, tag: str, keys: deque, throttler):
    async with throttler:
        keys.rotate(1)
        async with session.get(url, headers={'Authorization': f'Bearer {keys[0]}'}) as response:
            if response.status == 200:
                return (await response.read(), tag)
            return (None, None)
