from typing import TYPE_CHECKING, cast

from anyio.from_thread import BlockingPortalProvider
from asyncstdlib.functools import lru_cache as lru_acache

from .config import Config
from .serialization import PicklingZstdSerializer

if TYPE_CHECKING:  # pragma: no cover
    from typing import Any, ClassVar
    from uuid import UUID

    from redis.asyncio import Redis

    from .executor import Executor
    from .flow import Flow, FlowExecutionPlan
    from .io import IO
    from .serialization import Serializer


class Supervisor:
    _instance: "ClassVar[Supervisor | None]" = None

    def __init__(
        self,
        client: "Redis[bytes]",
        executor: "Executor",
        serializer: "Serializer | None" = None,
        async_config: dict[str, "Any"] = None,
        **settings: "Any",
    ) -> None:
        if async_config is None:
            async_config = {}

        self.config = Config(**settings)
        self.client: "Redis[bytes]" = client
        self.executor: "Executor" = executor
        self.serializer: "Serializer" = serializer or PicklingZstdSerializer(
            self.config.serialization_secret
        )
        self.async_portal = BlockingPortalProvider(
            backend=async_config.get("backend", "asyncio"), backend_options=async_config
        )

    @classmethod
    def instance(cls) -> "Supervisor":
        """Get the global Supervisor instance."""
        if cls._instance is None:
            raise RuntimeError(
                "Supervisor is not available. Call Supervisor.attach() first."
            )

        return cls._instance

    @classmethod
    def attach(cls, *args: "Any", **kwargs: "Any") -> "Supervisor":
        """Create or replace the global Supervisor instance."""
        instance = cls(*args, **kwargs)
        cls._instance = instance
        return instance

    async def start_flow(
        self,
        flow: "Flow",
        inputs: list["IO[Any]"] | None = None,
        all_outputs: bool = False,
    ) -> dict[str, "IO[Any]"]:
        return await self.executor.start(flow, inputs=inputs, all_outputs=all_outputs)

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
