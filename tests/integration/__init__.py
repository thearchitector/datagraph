import time
from collections.abc import AsyncIterator

import anyio
import pytest

from datagraph import IO, Flow, IOVal, Processor

# define a processor with a name 'foo'
# that expects some input stream 'a'
# and outputs two streams 'b' and 'c'
foo = Processor(name="foo", inputs={"a"}, outputs={"b", "c"})


@foo
async def _foo(a: IO[int]) -> AsyncIterator[IOVal[int]]:
    a_val = await a.first()

    yield IOVal(name="b", value=sum(range(a_val)))

    for i in range(a_val):
        yield IOVal(name="c", value=i)
        await anyio.lowlevel.checkpoint()


# define a processor named 'bar'
# that expects streams 'b' and 'c'
# and outputs a stream 'd'
bar = Processor(name="bar", inputs={"b", "c"}, outputs={"d"})


@bar
async def _bar(b: IO[int], c: IO[int]) -> AsyncIterator[IOVal[int]]:
    b_val = await b.first()

    async for c_val in c.stream():
        yield IOVal(name="d", value=b_val + c_val)
        await anyio.lowlevel.checkpoint()


# define some processor foobar
# that expects 4 inputs streams 'a', 'b', 'c', 'd'
foobar = Processor(name="foobar", inputs={"a", "b", "c", "d"}, outputs={"e"})


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
    flow = Flow.from_processors(foo).resolve()

    with anyio.fail_after(1):
        outputs: dict[str, IO] = await supervisor.start_flow(
            flow, inputs=[IOVal(name="a", value=5)]
        )

    assert {"b", "c"} == outputs.keys()

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
    flow = Flow.from_processors(foo, bar).resolve()

    with anyio.fail_after(1):
        outputs: dict[str, IO] = await supervisor.start_flow(
            flow, inputs=[IOVal(name="a", value=5)]
        )

    assert "d" in outputs.keys()

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
    flow = Flow.from_processors(foo, bar, foobar).resolve()

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


# Define processors for testing interlaced execution
producer = Processor(name="producer", inputs={"input"}, outputs={"produced"})


@producer
async def _producer(input: IO[int]) -> AsyncIterator[IOVal[tuple[int, float]]]:
    count = await input.first()

    for i in range(count):
        yield IOVal(name="produced", value=(i, time.time()))
        await anyio.sleep(0.01)


consumer = Processor(name="consumer", inputs={"produced"}, outputs={"consumed"})


@consumer
async def _consumer(
    produced: IO[tuple[int, float]],
) -> AsyncIterator[IOVal[tuple[int, float]]]:
    async for val in produced.stream():
        yield IOVal(name="consumed", value=(val[0], time.time()))
        await anyio.sleep(0.01)


@pytest.mark.anyio
async def test_execute_flow_interlaced(supervisor):
    flow = Flow.from_processors(producer, consumer).resolve()

    with anyio.fail_after(2):
        outputs = await supervisor.start_flow(
            flow, inputs=[IOVal(name="input", value=5)], all_outputs=True
        )

    produced = [v async for v in outputs["produced"].stream()]
    consumed = [v async for v in outputs["consumed"].stream()]

    p_values = [v[0] for v in produced]
    c_values = [v[0] for v in consumed]
    p_timestamps = [v[1] for v in produced]
    c_timestamps = [v[1] for v in consumed]

    # check that both streams produced the same values in the same order
    expected_values = list(range(5))
    assert p_values == expected_values
    assert c_values == expected_values

    # the test case here is that the consumer and producer can be interlaced, aka
    # C can yield a value for immediate processing by P before C yields another.
    #
    # we record the timestamps for yielding from both C and P for each entry. if
    # they're truly interlaced, zipping those lists should produce a strictly
    # increasing series of timestamps, i.e. we should see CPCPCP. if C processed
    # all values before P, we'd see CCCPPP, and the sorted list would not be
    # identical to the raw zipped one
    timestamps = list(zip(p_timestamps, c_timestamps, strict=False))
    assert timestamps == sorted(timestamps)
