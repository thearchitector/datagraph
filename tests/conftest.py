import pytest
from fakeredis import FakeAsyncConnection
from redis.asyncio import ConnectionPool

from datagraph import LocalExecutor, Supervisor
from datagraph.redis.anyio import Redis


@pytest.fixture(scope="session", autouse=True)
async def supervisor():
    pool = ConnectionPool.from_url(
        "redis://localhost:6379/0", connection_class=FakeAsyncConnection
    )
    return Supervisor.attach(client=Redis.from_pool(pool), executor=LocalExecutor())


@pytest.fixture(
    params=[
        # pytest.param(("asyncio", {"use_uvloop": True}), id="asyncio+uvloop"),
        pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio"),
        # pytest.param(
        #     ("trio", {"restrict_keyboard_interrupt_to_checkpoints": True}), id="trio"
        # ),
    ],
    scope="session",
)
def anyio_backend(request):
    return request.param
