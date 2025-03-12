# Datagraph

A framework-agnostic asynchronous task library based on dataflow computing principles. With Datagraph, you can treat task results as streams of information to enable
real-time and parallel data processing.

Datagraph is framework-agnostic, meaning it is not tied to any particular messaging or distributed queue system. You can use it locally, with Celery, or with any other asynchronous function framework; all you need is a notion of "running a task" to implement an `Executor`.

Out-of-the-box, there are Executors available for local execution and Celery.

## Features

WIP

- async first
- distributed first
- decentralized
    - tasks can be implemented in different services
    - no central task broker or management service
- fastapi-style task dependency injection
- optimistic parallel processing via directed streams
    - since tasks operate on streams you can rely on high degrees of parallelism vs. sequential DAGs
    - it can still be used as a workflow engine/canvas through task partitioning (tasks can wait for complete input streams before starting)
- redis based
- not complicated
    - the API is intentionally minimal and simple, because there's no reason for it not to be (data pipelines are hard enough, why fight with the implementation you don't control?)
- visualization
    - sometimes it's easier to grasp and talk with graphs, so there's a built-in way to visualize how data moves through a Flow

## Installation

WIP

## Usage

This is a contrived example, but illustrates many of the basic features you'd want to use:

```python
from collections.abc import AsyncIterator
from typing import Annotated

from datagraph import Depends, IO, LocalExecutor, Flow, Supervisor, Task
from redis.asyncio import Redis

# some externally-defined 'thing'
def get_multiplier():
    return 2

# define some tasks with some named inputs and outputs
foobar = Task(
    name="foobar",
    inputs=["foo"],
    outputs=["bar"]
)

# register the task to some function.
# this doesn't necessarily need to exist in the same repo / application / service that starts the Flow
@foobar
async def _foobar(
    foo: IO[int],
    # multiplier is a non-IO dependency and will resolve properly
    multiplier: Annotated[int, Depends(get_multiplier)],
) -> AsyncIterator[IOVal[int]]:
    # fetch the first (and only) value in the 'foo' IO stream
    foo_val = await foo.first()

    # yield individual values into the IO stream 'bar'
    for i in range(foo_val):
        yield IOVal(name="bar", value=i * multiplier)


async def main():
    # create a flow from tasks
    flow = Flow.from_tasks(foobar)

    # resolve the Flow. this is required, and ensures the Flow is actually executable.
    # you can also do `.from_tasks(...).resolve()`
    flow.resolve()

    result: dict[str, IO[Any]] = await Supervisor.instance.start_flow(
        flow, 
        inputs=[IOVal(name="foo", value=5)]
    )
    
    # concurrently stream the results as they complete. this will loop until foobar
    # has yielded all it's values to the 'bar' IO stream
    async for val in result["bar"].stream():
        print(val)


# the Supervisor is a global singleton responsible for coordinating
# local task execution. it is the means through which you define
# the Datagraph Redis client, Executor, or other configuration options.
# this can be done anywhere in your application, either in the import scope
# or within some entrypoint, but MUST be defined in every service that provides
# an implementation for a Task.
#
# nothing _strictly_ requires the non-Redis argument to match across all task
# services, but handling that behavior is undefined / unsupported.
Supervisor.attach(client=Redis(), executor=LocalExecutor())

# for the sake of example, this is the same as starting a Flow from within
# some running application service / endpoint
anyio.run(main)
# >>> 0
# >>> 2
# >>> 4
# >>> 6
# >>> 8
```

## License

This library is licensed under the [BSD 3-Clause License](./LICENSE).
