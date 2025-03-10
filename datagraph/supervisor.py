from typing import TYPE_CHECKING, cast

from asyncstdlib.functools import lru_cache as lru_acache

from .config import Config
from .serialization import PicklingZstdSerializer

if TYPE_CHECKING:  # pragma: no cover
    from typing import Any, ClassVar, Self
    from uuid import UUID

    from redis.asyncio import Redis

    from .executor import Executor
    from .flow import Flow, FlowExecutionPlan
    from .io import IO
    from .serialization import Serializer


class Supervisor:
    _instance: "ClassVar[Supervisor | None]" = None

    def __new__(cls, *args: "Any", **kwargs: "Any") -> "Supervisor":
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)

        return cls._instance

    def __init__(
        self,
        client: "Redis[bytes]",
        executor: "Executor",
        serializer: "Serializer | None" = None,
        **settings: "Any",
    ) -> None:
        self.config = Config(**settings)
        self.client: "Redis[bytes]" = client
        self.executor: "Executor" = executor
        self.serializer: "Serializer" = serializer or PicklingZstdSerializer()

    @classmethod
    def attach(cls, *args: "Any", **kwargs: "Any") -> "Supervisor":
        return cls(*args, **kwargs)

    @classmethod
    def instance(cls: type["Self"]) -> "Self":
        if not cls._instance:
            raise RuntimeError("Supervisor is not available.")

        return cls._instance

    async def start_flow(
        self, flow: "Flow", inputs: list["IO[Any]"] | None = None
    ) -> dict[str, "IO[Any]"]:
        return await self.executor.start(flow, inputs)

    @lru_acache(maxsize=5)
    async def _load_flow_execution_plan(
        self, flow_execution_uuid: "UUID"
    ) -> "FlowExecutionPlan":
        plan: bytes | None = await self.client.get(f"flow:{flow_execution_uuid}")
        if plan is None:
            raise ValueError(
                f"Flow execution plan not found for UUID '{flow_execution_uuid}'."
            )

        return cast("FlowExecutionPlan", self.serializer.load(plan))
