import pytest

from datagraph import CyclicFlowError, Flow, Task, UnresolvedFlowError
from datagraph.exceptions import DuplicateIOError

foo = Task(name="foo", inputs={"a"}, outputs={"b"})
bar = Task(name="bar", inputs={"b"}, outputs={"a"})
foobar = Task(name="foobar", inputs={"a", "b"}, outputs={"c"})


@pytest.mark.anyio
async def test_resolution_required(executor):
    flow = Flow.from_tasks(foo)

    assert not flow.resolved

    with pytest.raises(UnresolvedFlowError):
        await executor.start(flow)

    resolved_flow = flow.resolve()

    assert resolved_flow is flow

    assert flow.resolved
    str(flow)


@pytest.mark.anyio
async def test_resolution_failure_cyclic():
    flow = Flow.from_tasks(foo, bar)

    with pytest.raises(CyclicFlowError):
        flow.resolve()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "tasks",
    (
        (foo,),
        (foo, foobar),
    ),
    ids=("simple", "complex"),
)
async def test_resolve(tasks):
    flow = Flow.from_tasks(*tasks).resolve()

    assert flow.topology
    assert flow.execution_plan


@pytest.mark.anyio
async def test_duplicate_output_validation():
    # Create two tasks that produce the same output
    task1 = Task(name="task1", outputs={"same_output"})
    task2 = Task(name="task2", outputs={"same_output"})

    flow = Flow.from_tasks(task1, task2)

    # Resolving the flow should raise a ValueError
    with pytest.raises(DuplicateIOError):
        flow.resolve()
