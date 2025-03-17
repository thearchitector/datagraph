import inspect
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

    TaskFn = Callable[..., AsyncIterator[IOVal[Any]] | Awaitable[IOVal[Any]]]


class Task(BaseModel):
    name: str
    inputs: frozenset[str] = Field(default_factory=frozenset)
    outputs: frozenset[str] = Field(default_factory=frozenset)
    wait: bool = False

    _runner: "TaskRunner | None" = None

    model_config = ConfigDict(extra="forbid", frozen=True)

    def __call__(self, fn: "TaskFn") -> "TaskRunner":
        self._runner = TaskRunner(task=self, fn=fn)
        return self._runner

    def __getstate__(self) -> dict[str, "Any"]:
        state = super().__getstate__()
        # the task runner is not serializable, nor would we want it to be
        state.get("__pydantic_private__", {}).pop("_runner", None)
        return state


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
def _get_resolved_fn(fn: "TaskFn") -> "TaskFn":
    return inject(fn)


@dataclass
class TaskRunner:
    task: Task
    fn: "TaskFn"

    def __post_init__(self) -> None:
        self.__name__ = self.task.name

    async def _prepare_inputs(
        self, flow_execution_plan: "FlowExecutionPlan"
    ) -> dict[str, IO]:
        input_names: set[str] = self.task.inputs

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
                f"Task {self.task.name} has unresolvable parameters: {missing_args}"
            )

        return resolved_io_args

    async def _run(self, flow_execution_uuid: "UUID") -> None:
        plan = await Supervisor.instance()._load_flow_execution_plan(
            flow_execution_uuid
        )
        inputs = await self._prepare_inputs(plan)

        task_fn: "TaskFn" = _get_resolved_fn(self.fn)
        resolved_fn = task_fn(**inputs)

        if isinstance(resolved_fn, solve_async_gen):
            resolved_outputs = {
                output: IO(name=output, flow_execution_plan=plan, read_only=False)
                for output in self.task.outputs
            }

            async for output in resolved_fn:
                await resolved_outputs[output.name].write(output)
                await anyio.lowlevel.checkpoint()

            async with anyio.create_task_group() as tg:
                for output in resolved_outputs.values():
                    tg.start_soon(output.complete)
        else:
            await resolved_fn

    def __call__(self, flow_execution_uuid: "UUID") -> "Awaitable[None] | None":
        try:
            sniffio.current_async_library()
            return self._run(flow_execution_uuid)
        except sniffio.AsyncLibraryNotFoundError:
            with Supervisor.instance().async_portal as portal:
                portal.call(self._run, flow_execution_uuid)
