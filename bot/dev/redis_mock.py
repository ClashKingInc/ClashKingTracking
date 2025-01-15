import pendulum as pend


class MockRedis:
    """Mock Redis for testing purposes."""

    def __init__(self):
        self.store = {}

    async def set(self, key, value):
        self.store[key] = value
        # print(f'[MOCK REDIS] Set key: {key}')

    async def get(self, key):
        # print(f'[MOCK REDIS] Get key: {key}')
        return self.store.get(key)

    async def keys(self, pattern):
        import fnmatch

        return [
            key for key in self.store.keys() if fnmatch.fnmatch(key, pattern)
        ]

    async def delete(self, key):
        print(f'[MOCK REDIS] Delete key: {key}')
        if key in self.store:
            del self.store[key]

    async def expireat(self, key, timestamp):
        # print(f'[MOCK REDIS] Expire key: {key} at timestamp: {timestamp}')
        # Simulate expiration by removing the key if the timestamp has passed
        if pend.now(tz=pend.UTC).int_timestamp >= timestamp:
            await self.delete(key)
