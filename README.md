# Datagraph

An asynchronous data processing library based on dataflows. Datagraph empowers you to design declaratively, replacing the notion of "tasks" and "pipelines" with "processors" and "flows" that treat IO as continuous streams of manipulatable information.

Datagraph is framework-agnostic, meaning it is not tied to any particular messaging or distributed queue system. You can use it locally, with Celery,
or with any other asynchronous function framework; all you need is a notion of "running a function by name" to implement an `Executor`.

Out-of-the-box, there are `Executors` available for local execution and [Celery](https://docs.celeryq.dev/en/stable/index.html).

## Features

WIP

- async first, anyio for trio/asyncio
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

## Rationale

For the most part, distributed task frameworks all share a core design tenant: tasks accept discrete inputs and produce discrete outputs.
That model works well for one-off operations, or chains of operations dealing with relatively minimal data; your workflow makes sense as a series of steps, and
finishes when the last step completes. If I have tasks A, B, and C, I can pass data into A, then it can be passed to B, then C.

In the discrete model, by definition, C cannot run until B is finished, and B cannot run until A is finished: it is serial. When you're dealing with lots of
data, or perhaps starting with data that isn't all readily available, that begins to pose a problem. If I'm building an ETL pipeline, and one day need to process 20,000 files (not unreasonable for large business), my users would probably be mad if they had to wait for all 20,000 files to finish processing before they ended up in the application.

The procedural way of dealing with this is usually three-fold, all for the purpose of stabily reducing time-to-completion:
1. Break up the 20,000 files into more manageable groups and run multiple pipelines.
2. Implement some mechanism for coordianting and managing those independent groups across the tasks.
    - In the example above, that would likely _also_ mean implementing some homebrew monitoring tool or integrating with something like OpenTelemetry.
3. Rent more and bigger hardware (from AWS, etc.) and utilize some Horizontal Autoscaling-like feature so you can run 100 As, 100 Bs, and 100 Cs.

All of that is fine, of course, assuming you can afford it. But, naturally, it comes with other problems that have significantly less obvious answers: what do you do when something fails? Do you ensure your tasks are idempotent and restart everything? Do you implement some partial-success, partial-failure model with a deadletter queue? Do you just pay for Azure Data Factory, or perhaps a team to figure out Apache Kafka?

With Datagraph's approach, you can be a lot more _declarative_. Instead of focusing on the mechanics of a workflow, in either execution or deployment, you can focus on what your workflow achieves. You don't write steps, but implement processors; steps accept and produce discrete values, processors manipulate **continuous streams**.

By treating data as continuous streams, you get three huge advantages:
1. Processors can manipulate data as soon as it becomes available, rather than waiting for all of it to exist before starting.
2. You don't have to worry about task order or scheduling. Everything that can run in parallel does, _automatically_.
3. Your resource requirements don't strictly grow with scale; processing as streams means you are easily free to process as much or as little as you want at any given time without increasing CPU or (more likely) RAM.

In this model, applications like ETL pipelines make a lot more sense. You can start processing individual files as they're uploaded, transform one while the previous is extracting, and don't need to design around buying an EC2 instance with enough RAM to store 20,000 bitmap-filled PDFs.

There's also a fourth benefit. If you're writing a complex application, or just love imperative programming, you'll probably want to be able to run asynchronous tasks regardless. You can use Datagraph for that too; the benefit to defining a workflow using streams is that you can generalize procedural tasks as processors dealing in streams of 1 value.

## Installation

WIP

## Usage

This is a contrived example, but illustrates many of the basic features you'd want to use:

```python
from collections.abc import AsyncIterator
from typing import Annotated

from datagraph import Depends, IO, LocalExecutor, Flow, Supervisor, Processor
from redis.asyncio import Redis

# some externally-defined 'thing'
def get_multiplier():
    return 2

# define some tasks with some named inputs and outputs
foobar = Processor(
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
# an implementation for a Processor.
#
# nothing _strictly_ requires the executor to match across all services, but properly handling
# that behavior is undefined / unsupported.
Supervisor.attach(redis_config={"url": "redis://localhost:6379/0"}, executor=LocalExecutor())

# for the sake of example, this is the same as starting a Flow from within
# some running application service / endpoint
anyio.run(main, backend="trio")
# >>> 0
# >>> 2
# >>> 4
# >>> 6
# >>> 8
```

## License

This library is licensed under the [BSD 3-Clause License](./LICENSE).
