import pytest
from fakeredis import FakeAsyncRedis

from datagraph import LocalExecutor, Supervisor


@pytest.fixture(scope="session")
async def redis_client():
    async with FakeAsyncRedis() as client:
        yield client


@pytest.fixture(scope="session")
def supervisor(redis_client):
    return Supervisor.attach(client=redis_client, executor=LocalExecutor())


@pytest.fixture
def anyio_backend():
    return "trio"
