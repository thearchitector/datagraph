from typing import TYPE_CHECKING

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
        for task in tasks:
            Supervisor.get_task_runner(task)(flow_execution_plan.uuid)
