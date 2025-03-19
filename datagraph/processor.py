import inspect
import warnings
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, get_origin

import anyio
import sniffio
from fast_depends import Depends, inject
from fast_depends.use import solve_async_gen
from pydantic import BaseModel, ConfigDict, Field

from .io import IO
from .supervisor import Supervisor

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import AsyncIterator, Awaitable, Callable
    from typing import Any
    from uuid import UUID

    from .flow import FlowExecutionPlan
    from .io import IOVal

    ProcessorFn = Callable[..., AsyncIterator[IOVal[Any]] | Awaitable[IOVal[Any]]]


class Processor(BaseModel):
    name: str
    inputs: frozenset[str] = Field(default_factory=frozenset)
    outputs: frozenset[str] = Field(default_factory=frozenset)
    wait: bool = False

    model_config = ConfigDict(extra="forbid", frozen=True)

    def __call__(self, fn: "ProcessorFn") -> "ProcessorRunner":
        runner = ProcessorRunner(processor=self, fn=fn)
        Supervisor.register_processor(self, runner)
        return runner


@lru_cache
def _get_available_parameters(fn) -> dict[str, dict[str, bool]]:
    init_signature = inspect.signature(fn)
    parameters = init_signature.parameters.values()
    return {
        param.name: {
            "annotation": param.annotation,
            "optional": param.default is not inspect.Parameter.empty,
        }
        for param in parameters
        if param.name != "self"
    }


@lru_cache(maxsize=None)
def _get_resolved_fn(fn: "ProcessorFn") -> "ProcessorFn":
    return inject(fn)


@dataclass
class ProcessorRunner:
    processor: Processor
    fn: "ProcessorFn"

    def __post_init__(self) -> None:
        self.__name__ = self.processor.name

    async def _prepare_inputs(
        self, flow_execution_plan: "FlowExecutionPlan"
    ) -> dict[str, IO]:
        input_names: set[str] = self.processor.inputs

        parameters = _get_available_parameters(self.fn)
        resolved_io_args: dict[str, IO] = {
            inp: IO(name=inp, flow_execution_plan=flow_execution_plan, read_only=True)
            for inp, param in parameters.items()
            if inp in input_names and get_origin(param["annotation"]) is IO
        }
        resolved_optional_args: set[str] = {
            name
            for name, param in parameters.items()
            if (
                # optional also captures dependencies defined as `a = Depends(_a)`
                param["optional"]
                or (
                    (meta := getattr(param["annotation"], "__metadata__", None))
                    and len(meta) == 2
                    and isinstance(meta[1], Depends)
                )
            )
        }

        if missing_args := (
            parameters.keys() - resolved_io_args.keys() - resolved_optional_args
        ):
            raise ValueError(
                f"Processor {self.processor.name} has unresolvable parameters: {missing_args}"
            )

        return resolved_io_args

    async def run(self, flow_execution_uuid: "UUID") -> None:
        plan = await Supervisor.instance().load_flow_execution_plan(
            flow_execution_uuid,
        )

        inputs = await self._prepare_inputs(plan)
        resolved_outputs: dict[str, IO["Any"]] = {
            output: IO(name=output, flow_execution_plan=plan, read_only=False)
            for output in self.processor.outputs
        }
        output_writes: dict[str, int] = {output: 0 for output in self.processor.outputs}

        processor_fn: "ProcessorFn" = _get_resolved_fn(self.fn)
        resolved_fn = processor_fn(**inputs)

        if isinstance(resolved_fn, solve_async_gen):
            async for output in resolved_fn:
                await resolved_outputs[output.name].write(output)
                output_writes[output.name] += 1
        else:
            await resolved_fn

        async with anyio.create_task_group() as tg:
            for output in resolved_outputs.values():
                if output_writes[output.name] == 0:
                    warnings.warn(f"Output IO '{output.name}' is empty.", stacklevel=2)

                tg.start_soon(output.complete)

        await Supervisor.instance().executor.advance(plan)

    def __call__(self, flow_execution_uuid: "UUID") -> "Awaitable[None]":
        try:
            sniffio.current_async_library()
            raise RuntimeError(
                "Calling processors directly within an event loop is forbidden as it relies"
                " on dispatching to an external event loop. Use `.run` instead."
            )
        except sniffio.AsyncLibraryNotFoundError:
            Supervisor.instance().async_portal.start_task_soon(
                self.run,
                flow_execution_uuid,
                name=f"{flow_execution_uuid}:{self.processor.name}",
            )
