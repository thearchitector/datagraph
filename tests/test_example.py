from collections.abc import AsyncIterator

import anyio
import pytest

from datagraph import IO, Flow, IODef, Task

logs = []

# define a task with a name 'foo'
# that expects some input stream 'a'
# and outputs two streams 'b' and 'c'
foo = Task(
    name="foo",
    inputs=[
        IODef(name="a"),
    ],
    outputs=[
        IODef(name="b"),
        IODef(name="c"),
    ],
)


@foo
async def _foo(a: IO[int]) -> AsyncIterator[IO[int]]:
    a_val = await a.first()

    yield IO(name="b", value=sum(range(a_val)))

    for i in range(a_val):
        logs.append(f"foo: {i}")
        yield IO(name="c", value=i)
        await anyio.lowlevel.checkpoint()


# define a task named 'bar'
# that expects streams 'b' and 'c'
# and outputs a stream 'd'
bar = Task(
    name="bar",
    inputs=[
        IODef(name="b"),
        IODef(name="c"),
    ],
    outputs=[
        IODef(name="d"),
    ],
)


@bar
async def _bar(b: IO[int], c: IO[int]) -> AsyncIterator[IO[int]]:
    b_val = await b.first()

    async for c_val in c.stream():
        logs.append(f"bar: {b_val + c_val}")
        yield IO(name="d", value=b_val + c_val)
        await anyio.lowlevel.checkpoint()


# define some task foobar
# that expects 4 inputs streams 'a', 'b', 'c', 'd'
foobar = Task(
    name="foobar",
    inputs=[
        IODef(name="a"),
        IODef(name="b"),
        IODef(name="c"),
        IODef(name="d"),
    ],
    outputs=[
        IODef(name="e"),
    ],
)


@foobar
async def _foobar(
    a: IO[int], b: IO[int], c: IO[int], d: IO[int]
) -> AsyncIterator[IO[int]]:
    a_val = await a.first()
    b_val = await b.first()

    # stream over both c and d at the same time so that we iterate over them even if
    # new data is blocked by c or d in any given loop. with pad=False we're asserting
    # that we can always do a 1-1 matchup of c and d. if pad=True, once either c or d
    # ran out of values, that shorter stream would yield Nones
    async for c_val, d_val in c.stream_with(d, pad=False):
        yield IO(name="e", value=a_val + b_val + c_val + d_val)
        await anyio.lowlevel.checkpoint()


@pytest.mark.anyio
async def test_execute_flow_simple(executor):
    flow = Flow.from_tasks(foo).resolve()

    outputs: dict[str, IO] = await executor.run(flow, inputs=[IO(name="a", value=5)])

    assert outputs.keys() == {"b", "c"}

    b_vals = [r async for r in outputs["b"].stream()]
    b_val = await outputs["b"].first()
    c_val = await outputs["c"].first()

    assert b_vals == [10]
    assert b_val == 10
    assert c_val == 0


@pytest.mark.anyio
async def test_execute_flow_interlacing(executor):
    flow = Flow.from_tasks(foo, bar).resolve()

    outputs: dict[str, IO] = await executor.run(flow, inputs=[IO(name="a", value=5)])

    assert outputs.keys() == {"d"}

    # wait for the flow to complete
    [_ async for _ in outputs["d"].stream()]

    assert logs == [
        "foo: 0",
        "bar: 10",
        "foo: 1",
        "bar: 11",
        "foo: 2",
        "bar: 12",
        "foo: 3",
        "bar: 13",
        "foo: 4",
        "bar: 14",
    ]


@pytest.mark.anyio
async def test_execute_flow_complex(executor):
    flow = Flow.from_tasks(foo, bar, foobar).resolve()

    outputs: dict[str, IO] = await executor.run(flow, inputs=[IO(name="a", value=5)])

    assert outputs.keys() == {"e"}

    e_vals = [r async for r in outputs["e"].stream()]
    expected = [
        5 + 10 + 0 + 10,
        5 + 10 + 1 + 11,
        5 + 10 + 2 + 12,
        5 + 10 + 3 + 13,
        5 + 10 + 4 + 14,
    ]
    assert e_vals == expected
