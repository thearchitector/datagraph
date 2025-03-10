# DataGraph

An asynchronous data framework based on dataflow programming principles. DataGraph allows you to define tasks as async iterators that can incrementally yield data, forming a Directed Acyclic Graph (DAG).

## Features

- **Async-first**: Built on anyio for asynchronous execution
- **Dataflow Programming**: Tasks can incrementally yield data in streams
- **Dependency Injection**: Tasks can specify IO requirements and consume outputs from previous tasks
- **Distributed Execution**: Run flows in either local or distributed contexts using Redis
- **Concurrent Execution**: Multiple tasks within a flow can run concurrently
- **No Central Management**: Flows are dispatched asynchronously without a central service

## Installation

```bash
pip install -e .
```

Or with Poetry:

```bash
poetry install
```

## Usage

Here's a simple example of how to use DataGraph:

```python
from collections.abc import AsyncIterator

from datagraph import IO, Executor, Flow, IODef, Task
from redis.asyncio import Redis

# Define a task with inputs and outputs
task1 = Task(
    name="task1",
    inputs=[IODef(name="input")],
    outputs=[IODef(name="output")],
)

@task1
async def _task1(input: IO[int]) -> AsyncIterator[IO[int]]:
    val = await input.first()
    yield IO(name="output", value=val * 2)

# Create a flow from tasks
flow = Flow.from_tasks(task1)
flow.resolve()

# Execute the flow
async def run():
    executor = Executor(redis=Redis())
    result = await executor.run(
        flow, 
        inputs=[IO(name="input", value=5)], 
        stream=True
    )
    
    # Stream results
    async for val in result.stream():
        print(val)
```

## License

MIT
