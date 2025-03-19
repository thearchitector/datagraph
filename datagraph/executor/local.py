from typing import TYPE_CHECKING

import anyio

from datagraph.supervisor import Supervisor

from .base import Executor

if TYPE_CHECKING:  # pragma: no cover
    from datagraph.flow import FlowExecutionPlan
    from datagraph.task import Task


class LocalExecutor(Executor):
    """
    An Executor for running Flows locally. All Tasks in the Flow must be defined in
    the current program context.
    """

    async def dispatch(
        self, flow_execution_plan: "FlowExecutionPlan", tasks: set["Task"]
    ) -> None:
        async with anyio.create_task_group() as tg:
            for task in tasks:
                tg.start_soon(
                    Supervisor.get_task_runner(task).run, flow_execution_plan.uuid
                )
