from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from glide import Batch

from datagraph.exceptions import (
    FloatingIOError,
    FlowExecutionAdvancementTimeout,
    UnresolvedFlowError,
)
from datagraph.io import IO
from datagraph.lock import LockError, ValkeyLock
from datagraph.supervisor import Supervisor

if TYPE_CHECKING:  # pragma: no cover
    from typing import Any

    from glide import TGlideClient

    from datagraph.flow import Flow
    from datagraph.flow_execution_plan import FlowExecutionPlan
    from datagraph.io import IOVal
    from datagraph.processor import Processor


class Executor(ABC):
    async def start(
        self,
        flow: "Flow",
        inputs: list["IOVal[Any]"] | None = None,
        all_outputs: bool = False,
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

        batch = Batch(is_atomic=True)

        # write initial values for inputs IOs
        input_ios: dict[str, IO["Any"]] = {
            inp.name: IO(inp.name, flow.execution_plan, read_only=False)
            for inp in inputs
        }
        for inp in inputs:
            await input_ios[inp.name].write(inp)

        # save the execution plan
        batch.set(
            f"flow:{flow.execution_plan.uuid}",
            Supervisor.instance().serializer.dump(flow.execution_plan),
        )

        await (await Supervisor.instance().client()).exec(batch, True)

        # kickoff the plan
        await self.advance(flow.execution_plan)

        if all_outputs:
            return {
                output: IO(output, flow.execution_plan, read_only=True)
                for p in flow.execution_plan.partitions
                for processor in p
                for output in processor.outputs
            }
        else:
            return {
                output: IO(output, flow.execution_plan, read_only=True)
                for processor in flow.execution_plan.partitions[-1]
                for output in processor.outputs
            }

    async def advance(self, flow_execution_plan: "FlowExecutionPlan") -> None:
        """
        Advances the Flow's execution plan by one partition if the current partition is
        complete.
        """
        client = await Supervisor.instance().client()

        if await flow_execution_plan.partition_complete():
            try:
                async with ValkeyLock(
                    client,
                    f"flow:{flow_execution_plan.uuid}:execution-lock",
                    blocking_timeout=(
                        Supervisor.instance().config.flow_execution_advancement_timeout
                    ),
                ):
                    partition: set["Processor"] = flow_execution_plan.proceed()

                    # save the execution plan
                    await client.set(
                        f"flow:{flow_execution_plan.uuid}",
                        Supervisor.instance().serializer.dump(flow_execution_plan),
                    )
            except LockError as e:
                raise FlowExecutionAdvancementTimeout(flow_execution_plan.uuid) from e
            except IndexError:
                await self._finish(client, flow_execution_plan)
            else:
                await self.dispatch(flow_execution_plan, partition)

    async def _finish(
        self, client: "TGlideClient", flow_execution_plan: "FlowExecutionPlan"
    ) -> None:
        await client.set(f"flow:{flow_execution_plan.uuid}:complete", b"true")

    @abstractmethod
    async def dispatch(
        self, flow_execution_plan: "FlowExecutionPlan", processors: set["Processor"]
    ) -> None:
        raise NotImplementedError()
