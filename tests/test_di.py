from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fast_depends import Depends

from datagraph import IO, Processor
from datagraph.flow import FlowExecutionPlan


def _generate_b() -> str:
    return "b"


foo = Processor(name="foo", inputs={"a"}, outputs={"b"})


@foo
async def _foo(a: IO[str], b: str = Depends(_generate_b)): ...


@pytest.mark.anyio
async def test_processor_di(supervisor, monkeypatch):
    mock_fep_uuid = uuid4()

    monkeypatch.setattr(
        supervisor,
        "_load_flow_execution_plan",
        AsyncMock(return_value=FlowExecutionPlan(uuid=mock_fep_uuid, partitions=[])),
    )
    await _foo.run(mock_fep_uuid)
