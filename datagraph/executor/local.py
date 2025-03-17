from typing import TYPE_CHECKING

from anyio import create_task_group

from .base import Executor

if TYPE_CHECKING:  # pragma: no cover
    from anyio.abc import TaskGroup

    from .flow import FlowExecutionPlan
    from .task import Task


class LocalExecutor(Executor):
    """
    An Executor for running Flows locally. All Tasks in the Flow must be defined in
    the current program context.

    Optionally accepts a task group into which the Tasks will be dispatched. If one is
    not provided, a new task group will be created and Flow execution will block.
    """

    def __init__(self, task_group: "TaskGroup | None" = None) -> None:
        self.task_group = task_group

    async def _dispatch(
        self,
        tg: "TaskGroup",
        flow_execution_plan: "FlowExecutionPlan",
        tasks: set["Task"],
    ) -> None:
        for task in tasks:
            if task._runner is None:
                raise ValueError(
                    f"Task '{task.name}' is not defined in the current context."
                    f" Register it to a function with `@{task.name}`."
                )

            tg.start_soon(task._runner, flow_execution_plan.uuid)

    async def dispatch(
        self, flow_execution_plan: "FlowExecutionPlan", tasks: set["Task"]
    ) -> None:
        if self.task_group:
            return await self._dispatch(self.task_group, flow_execution_plan, tasks)

        async with create_task_group() as tg:
            return await self._dispatch(tg, flow_execution_plan, tasks)
