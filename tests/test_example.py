from collections.abc import AsyncIterator

import anyio
import pytest

from datagraph import IO, Flow, IOVal, Task

# define a task with a name 'foo'
# that expects some input stream 'a'
# and outputs two streams 'b' and 'c'
foo = Task(name="foo", inputs={"a"}, outputs={"b", "c"})


@foo
async def _foo(a: IO[int]) -> AsyncIterator[IOVal[int]]:
    a_val = await a.first()

    yield IOVal(name="b", value=sum(range(a_val)))

    for i in range(a_val):
        yield IOVal(name="c", value=i)
        await anyio.lowlevel.checkpoint()


# define a task named 'bar'
# that expects streams 'b' and 'c'
# and outputs a stream 'd'
bar = Task(name="bar", inputs={"b", "c"}, outputs={"d"})


@bar
async def _bar(b: IO[int], c: IO[int]) -> AsyncIterator[IOVal[int]]:
    b_val = await b.first()

    async for c_val in c.stream():
        yield IOVal(name="d", value=b_val + c_val)
        await anyio.lowlevel.checkpoint()


# define some task foobar
# that expects 4 inputs streams 'a', 'b', 'c', 'd'
foobar = Task(name="foobar", inputs={"a", "b", "c", "d"}, outputs={"e"})


@foobar
async def _foobar(
    a: IO[int], b: IO[int], c: IO[int], d: IO[int]
) -> AsyncIterator[IOVal[int]]:
    a_val = await a.first()
    b_val = await b.first()

    # stream over both c and d at the same time so that we iterate over them even if
    # new data is blocked by c or d in any given loop. we're asserting
    # that we can always do a 1-1 matchup of c and d. if pad=True, once either c or d
    # ran out of values, that shorter stream would yield Nones
    async for c_val, d_val in c.stream_with(d):
        yield IOVal(name="e", value=a_val + b_val + c_val + d_val)
        await anyio.lowlevel.checkpoint()


@pytest.mark.anyio
async def test_execute_flow_simple(supervisor):
    flow = Flow.from_tasks(foo).resolve()

    with anyio.fail_after(1):
        outputs: dict[str, IO] = await supervisor.start_flow(
            flow, inputs=[IOVal(name="a", value=5)]
        )

    assert {"b", "c"}.issubset(outputs.keys())

    b_vals = [r async for r in outputs["b"].stream()]
    b_val = await outputs["b"].first()
    c_val_first = await outputs["c"].first()
    c_val_last = [r async for r in outputs["c"].stream()][-1]
    # c_val_latest = await outputs["c"].latest()

    assert b_vals == [10]
    assert b_val == 10
    assert c_val_first == 0
    assert c_val_last == 4
    # assert c_val_latest == 4


@pytest.mark.anyio
async def test_execute_flow_dual_output(supervisor):
    flow = Flow.from_tasks(foo, bar).resolve()

    with anyio.fail_after(1):
        outputs: dict[str, IO] = await supervisor.start_flow(
            flow, inputs=[IOVal(name="a", value=5)]
        )

    assert {"c", "d"}.issubset(outputs.keys())

    c_d_res = [rs async for rs in outputs["c"].stream_with(outputs["d"])]
    assert c_d_res == [
        (0, 10),
        (1, 11),
        (2, 12),
        (3, 13),
        (4, 14),
    ]


@pytest.mark.anyio
async def test_execute_flow_complex(supervisor):
    flow = Flow.from_tasks(foo, bar, foobar).resolve()

    with anyio.fail_after(1):
        outputs: dict[str, IO] = await supervisor.start_flow(
            flow, inputs=[IOVal(name="a", value=5)]
        )

        assert "e" in outputs.keys()

    e_vals = [r async for r in outputs["e"].stream()]
    assert e_vals == [
        5 + 10 + 0 + 10,
        5 + 10 + 1 + 11,
        5 + 10 + 2 + 12,
        5 + 10 + 3 + 13,
        5 + 10 + 4 + 14,
    ]
