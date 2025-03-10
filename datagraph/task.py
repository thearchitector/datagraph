import inspect
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, get_origin

from fast_depends import Depends, inject
from pydantic import BaseModel, ConfigDict, Field

from .io import IO
from .supervisor import Supervisor

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import AsyncIterator, Callable
    from typing import ParamSpec
    from uuid import UUID

    from .flow import FlowExecutionPlan

    P = ParamSpec("P")


class Task(BaseModel):
    name: str
    inputs: frozenset[str] = Field(default_factory=frozenset)
    outputs: frozenset[str] = Field(default_factory=frozenset)
    wait: bool = False

    _instance: "TaskInstance | None" = None

    model_config = ConfigDict(extra="forbid", frozen=True)

    def __call__(self, fn: "Callable[..., AsyncIterator[IO]]") -> "TaskInstance":
        self._instance = TaskInstance(task=self, fn=fn)
        return self._instance


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
def _get_resolved_fn(
    fn: "Callable[P, AsyncIterator[IO]]",
) -> "Callable[P, AsyncIterator[IO]]":
    return inject(fn)


@dataclass
class TaskInstance:
    task: Task
    fn: "Callable[..., AsyncIterator[IO]]"

    async def _prepare_inputs(
        self, flow_execution_plan: "FlowExecutionPlan"
    ) -> dict[str, IO]:
        input_names: set[str] = {inp.name for inp in self.task.inputs}

        parameters = _get_available_parameters(self.fn)
        resolved_io_args: dict[str, IO] = {
            inp: IO(name=inp)
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

    async def __call__(self, flow_execution_uuid: "UUID") -> None:
        plan = await Supervisor.instance._load_flow_execution_plan(flow_execution_uuid)
        inputs = await self._prepare_inputs(plan)

        async for output in _get_resolved_fn(self.fn)(**inputs):
            print(output)
