from typing import TYPE_CHECKING

import anyio

from datagraph.supervisor import Supervisor

from .base import Executor

if TYPE_CHECKING:  # pragma: no cover
    from datagraph.flow import FlowExecutionPlan
    from datagraph.processor import Processor


class LocalExecutor(Executor):
    """
    An Executor for running Flows locally. All Processors in the Flow must be defined in
    the current program context.
    """

    async def dispatch(
        self, flow_execution_plan: "FlowExecutionPlan", processors: set["Processor"]
    ) -> None:
        async with anyio.create_task_group() as tg:
            for processor in processors:
                tg.start_soon(
                    Supervisor.get_processor_runner(processor).run,
                    flow_execution_plan.uuid,
                )
