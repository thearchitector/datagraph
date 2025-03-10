from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from anyio import create_task_group
from redis.exceptions import LockError

from .exceptions import FlowExecutionAdvancementTimeout, UnresolvedFlowError
from .supervisor import Supervisor

if TYPE_CHECKING:  # pragma: no cover
    from typing import Any

    from anyio.abc import TaskGroup
    from redis.asyncio.client import Pipeline

    from .flow import Flow, FlowExecutionPlan
    from .io import IO
    from .task import Task


class Executor(ABC):
    async def start(
        self, flow: "Flow", inputs: list["IO[Any]"] = None
    ) -> dict[str, "IO[Any]"]:
        """Start a Flow."""
        if not flow.resolved:
            raise UnresolvedFlowError()

        await self._advance(flow.execution_plan)

        return {
            output: IO(output, flow.execution_plan)
            for task in flow.execution_plan.partitions[-1]
            for output in task.outputs
        }

    async def _advance(self, flow_execution_plan: "FlowExecutionPlan") -> None:
        """
        Advances the Flow's execution plan by one partition if the current partition is
        complete.
        """
        try:
            async with Supervisor.instance.client.lock(
                f"flow:{flow_execution_plan.uuid}:execution-lock",
                blocking=True,
                block_timeout=(
                    Supervisor.instance.config.flow_execution_advancement_timeout
                ),
            ):
                async with Supervisor.instance.client.pipeline() as pipe:
                    if await flow_execution_plan.partition_complete(pipe):
                        try:
                            partition: set["Task"] = flow_execution_plan.proceed(pipe)
                            await self.dispatch(flow_execution_plan, partition)
                        except IndexError:
                            await self._finish(pipe, flow_execution_plan)
        except LockError as e:
            raise FlowExecutionAdvancementTimeout(flow_execution_plan.uuid) from e

    async def _finish(
        self, pipeline: "Pipeline", flow_execution_plan: "FlowExecutionPlan"
    ) -> None:
        await pipeline.set(f"flow:{flow_execution_plan.uuid}:complete", True)

    @abstractmethod
    async def dispatch(
        self, flow_execution_plan: "FlowExecutionPlan", tasks: set["Task"]
    ) -> None:
        raise NotImplementedError()


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
            if task._instance is None:
                raise ValueError(
                    f"Task '{task.name}' is not defined in the current context."
                    f" Register it to a function with `@{task.name}`."
                )

            tg.start_soon(task._instance, flow_execution_plan.uuid)

    async def dispatch(
        self, flow_execution_plan: "FlowExecutionPlan", tasks: set["Task"]
    ) -> None:
        if self.task_group:
            return await self._dispatch(self.task_group, flow_execution_plan, tasks)

        async with create_task_group() as tg:
            return await self._dispatch(tg, flow_execution_plan, tasks)
