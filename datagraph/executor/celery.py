from typing import TYPE_CHECKING

from .base import Executor

if TYPE_CHECKING:  # pragma: no cover
    from celery import Celery

    from .flow import FlowExecutionPlan
    from .task import Task


class CeleryExecutor(Executor):
    def __init__(self, app: "Celery") -> None:
        self.celery_app = app

    async def dispatch(
        self, flow_execution_plan: "FlowExecutionPlan", tasks: set["Task"]
    ) -> None:
        for task in tasks:
            self.celery_app.send_task(
                task.name,
                args=[flow_execution_plan.uuid],
            )
