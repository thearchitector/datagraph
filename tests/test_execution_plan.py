import pytest

from datagraph import Flow, Task
from datagraph.execution_plan import ExecutionPlan


@pytest.mark.anyio
async def test_create_execution_plan():
    foo = Task(name="foo", outputs={"a", "b"})
    bar = Task(name="bar", inputs={"a"}, outputs={"c"})
    foobar = Task(name="foobar", inputs={"b"}, outputs={"d"})
    buzz = Task(name="buzz", inputs={"d"}, wait=True)

    flow = Flow.from_tasks(foo, bar, foobar, buzz).resolve()

    assert flow.execution_plan == ExecutionPlan(partitions=[{foo, bar, foobar}, {buzz}])
