from unittest.mock import AsyncMock, patch

import pytest

pytest_plugins = ("celery.contrib.pytest",)


@pytest.fixture
async def supervisor():
    # Use AsyncMock for the Supervisor.instance() mock so it can be awaited
    with patch(
        "datagraph.supervisor.Supervisor.instance", return_value=AsyncMock()
    ) as mock_supervisor:
        yield mock_supervisor


@pytest.fixture(
    params=[
        pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio"),
        pytest.param(
            ("trio", {"restrict_keyboard_interrupt_to_checkpoints": True}), id="trio"
        ),
    ],
    scope="session",
)
def anyio_backend(request):
    return request.param
