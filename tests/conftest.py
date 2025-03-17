import anyio
import pytest
from fakeredis import FakeAsyncConnection
from redis.asyncio import ConnectionPool

from datagraph import LocalExecutor, Supervisor
from datagraph.redis.anyio import Redis


@pytest.fixture(scope="session")
def redis_pool():
    return ConnectionPool.from_url(
        "redis://localhost:6379/0", connection_class=FakeAsyncConnection
    )


@pytest.fixture(scope="module", autouse=True)
async def supervisor(redis_pool):
    async with anyio.create_task_group() as tg:
        yield Supervisor.attach(
            client=Redis.from_pool(redis_pool), executor=LocalExecutor(tg)
        )


@pytest.fixture(
    params=[
        pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio"),
        # pytest.param(("asyncio", {"use_uvloop": True}), id="asyncio+uvloop"),
        # pytest.param(
        #     ("trio", {"restrict_keyboard_interrupt_to_checkpoints": True}), id="trio"
        # ),
    ],
    scope="session",
)
def anyio_backend(request):
    return request.param
