import time
from collections.abc import AsyncIterator

import anyio

from datagraph import IO, IOVal, Task

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


# define a task named 'bar'
# that expects streams 'b' and 'c'
# and outputs a stream 'd'
bar = Task(name="bar", inputs={"b", "c"}, outputs={"d"})


@bar
async def _bar(b: IO[int], c: IO[int]) -> AsyncIterator[IOVal[int]]:
    b_val = await b.first()

    async for c_val in c.stream():
        yield IOVal(name="d", value=b_val + c_val)


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


producer = Task(name="producer", inputs={"input"}, outputs={"produced"})


@producer
async def _producer(input: IO[int]) -> AsyncIterator[IOVal[tuple[int, float]]]:
    count = await input.first()

    for i in range(count):
        yield IOVal(name="produced", value=(i, time.time()))
        await anyio.sleep(0.01)


consumer = Task(name="consumer", inputs={"produced"}, outputs={"consumed"})


@consumer
async def _consumer(
    produced: IO[tuple[int, float]],
) -> AsyncIterator[IOVal[tuple[int, float]]]:
    async for val in produced.stream():
        yield IOVal(name="consumed", value=(val[0], time.time()))
        await anyio.sleep(0.01)
