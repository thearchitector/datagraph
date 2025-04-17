import pytest

from datagraph import Flow, Processor
from datagraph.exceptions import CyclicFlowError, DuplicateIOError, UnresolvedFlowError

foo = Processor(name="foo", inputs={"a"}, outputs={"b"})
bar = Processor(name="bar", inputs={"b"}, outputs={"a"})
foobar = Processor(name="foobar", inputs={"a", "b"}, outputs={"c"})


@pytest.mark.anyio
async def test_resolution_required(supervisor):
    flow = Flow.from_processors(foo)

    assert not flow.resolved

    with pytest.raises(UnresolvedFlowError):
        str(flow.topology)

    resolved_flow = flow.resolve()

    assert resolved_flow is flow

    assert flow.resolved
    assert str(flow.topology)


@pytest.mark.anyio
async def test_resolution_failure_cyclic():
    flow = Flow.from_processors(foo, bar)

    with pytest.raises(CyclicFlowError):
        flow.resolve()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "processors",
    (
        (foo,),
        (foo, foobar),
    ),
    ids=("simple", "complex"),
)
async def test_resolve(processors):
    flow = Flow.from_processors(*processors).resolve()

    assert flow.topology
    assert flow.execution_plan


@pytest.mark.anyio
async def test_duplicate_output_validation():
    # Create two processors that produce the same output
    processor1 = Processor(name="processor1", outputs={"same_output"})
    processor2 = Processor(name="processor2", outputs={"same_output"})

    flow = Flow.from_processors(processor1, processor2)

    # Resolving the flow should raise a ValueError
    with pytest.raises(DuplicateIOError):
        flow.resolve()
