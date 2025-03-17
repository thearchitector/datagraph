import pytest
from fakeredis import FakeAsyncConnection
from redis.asyncio import ConnectionPool


@pytest.fixture
async def redis_pool():
    try:
        pool = ConnectionPool.from_url(
            "redis://localhost:6379/0", connection_class=FakeAsyncConnection
        )
        yield pool
    finally:
        await pool.disconnect()
