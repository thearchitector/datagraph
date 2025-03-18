from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from redis.exceptions import LockError

from datagraph.exceptions import (
    FloatingIOError,
    FlowExecutionAdvancementTimeout,
    UnresolvedFlowError,
)
from datagraph.io import IO
from datagraph.supervisor import Supervisor

if TYPE_CHECKING:  # pragma: no cover
    from typing import Any

    from redis.asyncio.client import Redis

    from datagraph.flow import Flow, FlowExecutionPlan
    from datagraph.io import IOVal
    from datagraph.task import Task


class Executor(ABC):
    async def start(
        self, flow: "Flow", inputs: list["IOVal[Any]"] = None, all_outputs: bool = False
    ) -> dict[str, "IO[Any]"]:
        """Start a Flow."""
        if inputs is None:
            inputs = []

        # ensure the Flow is runnable
        if not flow.resolved:
            raise UnresolvedFlowError()
        elif missing_inputs := (
            flow.topology.floating_inputs - {inp.name for inp in inputs}
        ):
            raise FloatingIOError(missing_inputs)

        async with Supervisor.instance().client.pipeline() as pipe:
            # write initial values for inputs IOs
            input_ios: dict[str, IO["Any"]] = {
                inp.name: IO(inp.name, flow.execution_plan, read_only=False)
                for inp in inputs
            }
            for inp in inputs:
                await input_ios[inp.name].write(inp)

            # save the execution plan
            await pipe.set(
                f"flow:{flow.execution_plan.uuid}",
                Supervisor.instance().serializer.dump(flow.execution_plan),
            )

            await pipe.execute()

        # kickoff the plan
        await self.advance(flow.execution_plan)

        if all_outputs:
            return {
                output: IO(output, flow.execution_plan, read_only=True)
                for p in flow.execution_plan.partitions
                for task in p
                for output in task.outputs
            }
        else:
            return {
                output: IO(output, flow.execution_plan, read_only=True)
                for task in flow.execution_plan.partitions[-1]
                for output in task.outputs
            }

    async def advance(self, flow_execution_plan: "FlowExecutionPlan") -> None:
        """
        Advances the Flow's execution plan by one partition if the current partition is
        complete.
        """

        if await flow_execution_plan.partition_complete():
            try:
                async with Supervisor.instance().client.lock(
                    f"flow:{flow_execution_plan.uuid}:execution-lock",
                    blocking=True,
                    blocking_timeout=(
                        Supervisor.instance().config.flow_execution_advancement_timeout
                    ),
                ):
                    partition: set["Task"] = flow_execution_plan.proceed()

                    # save the execution plan
                    await Supervisor.instance().client.set(
                        f"flow:{flow_execution_plan.uuid}",
                        Supervisor.instance().serializer.dump(flow_execution_plan),
                    )
            except LockError as e:
                raise FlowExecutionAdvancementTimeout(flow_execution_plan.uuid) from e
            except IndexError:
                await self._finish(Supervisor.instance().client, flow_execution_plan)
            else:
                await self.dispatch(flow_execution_plan, partition)

    async def _finish(
        self, client: "Redis[bytes]", flow_execution_plan: "FlowExecutionPlan"
    ) -> None:
        await client.set(f"flow:{flow_execution_plan.uuid}:complete", b"true")

    @abstractmethod
    async def dispatch(
        self, flow_execution_plan: "FlowExecutionPlan", tasks: set["Task"]
    ) -> None:
        raise NotImplementedError()
