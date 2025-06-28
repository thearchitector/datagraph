# Datagraph

A distributed asynchronous Python data processing framework based on dataflows. Datagraph empowers you to design declaratively, abstracting the notion of discrete "tasks" and "queues" into "processors" and "flows" that treat IO as continuous streams of manipulatable information.

Datagraph is lightweight and infrastructure-agnostic: it is not tied to any particular messaging or distributed queue system. You can use it locally, with Celery,
or with any other asynchronous function framework; all you need is a notion of "running a function by name" to implement an `Executor`.

Out-of-the-box, there are `Executors` available for local execution and [Celery](https://docs.celeryq.dev/en/stable/index.html).

The only runtime dependency is a RESP3-compliant key/value store. Primary support is for [Valkey](https://valkey.io/).

> See [Why?](#why) for a more in-depth walk through of Datagraph's advantages.

### Features

- Async-first, with support for both Trio and asyncio.
- Lightweight and infrastructure-agnostic.
    - No central task broker or management service.
    - You only need a RESP3-compliant key/value store.
- Distributed and decentralized.
    - Processors can be implemented in different services.
- FastAPI-style processor dependency injection.
- Optimistic parallel processing via directed streams.
    - Since processors operate on streams, you can rely on high degrees of parallelism vs. sequential DAGs.
    - Supports use as a discrete task canvas through partitioning (processors can wait for complete input streams before starting).
- Not complicated.
    - The API is intentionally small and simple, because there's no reason for it not to be (data pipelines are hard enough, why fight with the implementation you don't control?)
- Visualization
    - Sometimes it's easier to grasp and talk with graphs, so there's a built-in way to visualize how data moves through a Flow.

### Installation

```bash
pdm add datagraph
# or
pip install datagraph
```

### Usage

This is a contrived example, but illustrates many of the basic features you'd want to use:

```python
from collections.abc import AsyncIterator
from typing import Annotated

from datagraph import Depends, IO, LocalExecutor, Flow, Supervisor, Processor
from redis.asyncio import Redis

# some externally-defined 'thing'
def get_multiplier():
    return 2

# define some processors with some named inputs and outputs
foobar = Processor(
    name="foobar",
    inputs=["foo"],
    outputs=["bar"]
)

# register the processor to some function.
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
    # create a flow from processors
    flow = Flow.from_processors(foobar)

    # resolve the Flow. this is required, and ensures the Flow is actually executable.
    # you can also do `.from_processors(...).resolve()`
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
# local processor execution. it is the means through which you define
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

## Why?

### Why Streams?

For the most part, distributed task frameworks all share a core design tenant: tasks accept discrete inputs and produce discrete outputs.
That model works well for one-off operations, or chains of operations dealing with relatively minimal data; your workflow makes sense as a series of steps, and
finishes when the last step completes. If you have tasks A, B, and C, you can pass data into A, then it can be passed to B, then C.

In the discrete model, by definition, C cannot run until B is finished, and B cannot run until A is finished: it is serial. When you're dealing with lots of
data, or perhaps starting with data that isn't readily available in entirety, that restriction begins to pose a problem. If you're building an ETL pipeline, and one day need to process 20,000 files (not unreasonable for large business), your users would likely be upset if they had to wait for all 20,000 files to finish processing in steps A and B before they were loaded into the application by step C.

The procedural way of dealing with this would be three-fold, all for the purpose of stabily reducing time-to-completion:
1. Break up the 20,000 files into more manageable groups, then run multiple pipelines.
2. Implement some mechanism for coordianting and managing those independent groups.
3. Rent more, bigger, and faster hardware (from AWS, etc.) and utilize some Horizontal Autoscaling-like feature so you can run 100 As, 100 Bs, and 100 Cs.

All of that is fine, of course, assuming you can afford it. But, naturally, it comes with other problems that have significantly less obvious answers: what do you do when something fails? Do you ensure your tasks are idempotent and restart everything? Do you implement some partial-success, partial-failure model with a deadletter queue? The list can go on.

Streams solve these problems by enabling data to flow through your pipeline continuously. Instead of waiting for entire batches to complete, each piece of data moves through the system as soon as it's ready, with tasks C, B, and A all running in parallel on different pieces of data. The result? Lower latency, better resource utilization, and real-time results.

### Why Datagraph?

While many stream processing frameworks exist, most come with significant complexity and operational overhead. Datagraph offers a fundamentally different approach:

1. **No external runtime or complex infrastructure**
   - Unlike Kafka/Flink/Spark, Datagraph is pure Python with a single RESP3 store dependency.
   - No JVM tuning, no Zookeeper clusters, no dedicated stream processing servers, fewer headaches.

2. **Developer experience first**
   - FastAPI-style dependency injection with a clean, declarative API.
   - Define processors with simple decorators.
   - Designed around async generators to match the continuous dataflow model.
   - Supports both Trio and asyncio.

3. **Infrastructure freedom**
   - Pluggable executor architecture lets you run anywhere.
   - Start with `LocalExecutor` and switch to Celery, Kubernetes, or serverless without code changes.
   - No platform or vendor lock-in. Most of your time can be spent on the logic, not the infra.

4. **Right-sized for practical workloads**
   - Optimized for the 10K-100K messages/sec range on commodity hardware
   - Perfect for ETL, IoT ingestion, and real-time dashboards
   - Built-in backpressure prevents fast producers from overwhelming slow consumers

5. **Visualization included**
   - Built-in graph rendering shows data flow at a glance
   - Diagnose bottlenecks visually rather than through logs

Datagraph doesn't aim to replace Apache Beam or Flink for petabyte-scale analytics. Instead, it targets the vast middle ground where teams need streaming capabilities without the operational complexity of distributed stream processing clusters.

By focusing on **simplicity**, **Python-native ergonomics**, and **just-enough streaming**, Datagraph lets you build, test, and scale real-time pipelines in minutes—not months.

## License

This library is licensed under the [BSD 3-Clause License](./LICENSE).
