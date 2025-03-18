from typing import TYPE_CHECKING

from .base import Executor

if TYPE_CHECKING:  # pragma: no cover
    from celery import Celery

    from datagraph.flow import FlowExecutionPlan
    from datagraph.task import Task


class CeleryExecutor(Executor):
    """
    An Executor for running Flows via Celery. All Tasks in the Flow must be registered
    as Celery tasks in the worker that implements them.

    ```python
    celery_app = Celery()
    Supervisor.attach(
        redis_config={"url": "redis://localhost:6379/0"},
        executor=CeleryExecutor(celery_app),
    )

    foo = Task(name="foo", inputs={"a"}, outputs={"b"})

    @celery_app.task(foo.name, ignore_result=True)
    @foo
    async def _foo(a: IO[int]) -> AsyncIterator[IOVal[int]]: ...
    ```
    """

    def __init__(self, app: "Celery") -> None:
        self.celery_app = app

    async def dispatch(
        self, flow_execution_plan: "FlowExecutionPlan", tasks: set["Task"]
    ) -> None:
        for task in tasks:
            self.celery_app.send_task(task.name, args=[flow_execution_plan.uuid])
