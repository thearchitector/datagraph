import pytest

from datagraph import Flow, Processor
from datagraph.flow import FlowExecutionPlan


@pytest.mark.anyio
async def test_create_execution_plan():
    foo = Processor(name="foo", outputs={"a", "b"})
    bar = Processor(name="bar", inputs={"a"}, outputs={"c"})
    foobar = Processor(name="foobar", inputs={"b"}, outputs={"d"})
    buzz = Processor(name="buzz", inputs={"d"}, wait=True)

    flow = Flow.from_processors(foo, bar, foobar, buzz).resolve()

    assert flow.execution_plan == FlowExecutionPlan(
        uuid=flow.execution_plan.uuid, partitions=[{foo, bar, foobar}, {buzz}]
    )
