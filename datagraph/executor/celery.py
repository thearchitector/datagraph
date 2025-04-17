from typing import TYPE_CHECKING

from .base import Executor

try:
    from celery import Celery
except ImportError:  # pragma: no cover
    raise RuntimeError("Celery is required to use the CeleryExecutor.") from None

if TYPE_CHECKING:  # pragma: no cover
    from datagraph.flow_execution_plan import FlowExecutionPlan
    from datagraph.processor import Processor


class CeleryExecutor(Executor):
    """
    An Executor for running Flows via Celery. All Processors in the Flow must be registered
    as Celery processors in the worker that implements them.

    ```python
    celery_app = Celery()
    Supervisor.attach(
        redis_config={"url": "redis://localhost:6379/0"},
        executor=CeleryExecutor(celery_app),
    )

    foo = Processor(name="foo", inputs={"a"}, outputs={"b"})

    @celery_app.task(foo.name, ignore_result=True)
    @foo
    async def _foo(a: IO[int]) -> AsyncIterator[IOVal[int]]: ...
    ```
    """

    def __init__(self, app: "Celery") -> None:
        self.celery_app = app

    async def dispatch(
        self, flow_execution_plan: "FlowExecutionPlan", processors: set["Processor"]
    ) -> None:
        for processor in processors:
            self.celery_app.send_task(processor.name, args=[flow_execution_plan.uuid])
