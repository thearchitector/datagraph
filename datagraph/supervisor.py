import warnings
from typing import TYPE_CHECKING, cast

from anyio.from_thread import start_blocking_portal
from anyio.lowlevel import RunVar
from redis.asyncio import Redis

from .config import Config
from .exceptions import UnregisteredProcessorError
from .serialization import PicklingZstdSerializer

if TYPE_CHECKING:  # pragma: no cover
    from typing import Any, ClassVar
    from uuid import UUID

    from .executor import Executor
    from .flow import Flow
    from .flow_execution_plan import FlowExecutionPlan
    from .io import IO
    from .processor import Processor, ProcessorRunner
    from .serialization import Serializer


class Supervisor:
    _instance: "ClassVar[Supervisor | None]" = None
    _processors: "ClassVar[dict[Processor, ProcessorRunner]]" = {}

    def __init__(
        self,
        executor: "Executor",
        redis_config: dict[str, "Any"],
        serializer: "Serializer | None" = None,
        async_config: dict[str, "Any"] = None,
        **settings: "Any",
    ) -> None:
        if Supervisor._instance is not None:
            raise RuntimeError("Supervisor is already attached.")

        if async_config is None:
            async_config = {}

        self.config = Config(**settings)

        self._redis_config = redis_config
        self._client_var: RunVar["Redis[bytes]"] = RunVar("_client_var")

        self.executor: "Executor" = executor
        self.serializer: "Serializer" = serializer or PicklingZstdSerializer(
            self.config.serialization_secret
        )

        self._portal_cm = start_blocking_portal(
            async_config.pop("backend", "asyncio"), async_config
        )
        self.async_portal = self._portal_cm.__enter__()

    @classmethod
    def instance(cls) -> "Supervisor":
        """Get the global Supervisor instance."""
        if cls._instance is None:
            raise RuntimeError(
                "Supervisor is not available. Call Supervisor.attach() first."
            )

        return cls._instance

    @classmethod
    def attach(
        cls,
        executor: "Executor",
        redis_config: dict[str, "Any"],
        serializer: "Serializer | None" = None,
        async_config: dict[str, "Any"] = None,
        **settings: "Any",
    ) -> "Supervisor":
        instance = cls(
            executor,
            redis_config,
            serializer=serializer,
            async_config=async_config,
            **settings,
        )
        cls._instance = instance
        return instance

    @classmethod
    def register_processor(
        cls, processor: "Processor", runner: "ProcessorRunner"
    ) -> None:
        if processor in cls._processors:
            warnings.warn(
                f"Processor '{processor.name}' is already registered. This will override that"
                " implementation.",
                stacklevel=3,
            )

        cls._processors[processor] = runner

    @classmethod
    def get_processor_runner(cls, processor: "Processor") -> "ProcessorRunner":
        if runner := cls._processors.get(processor):
            return runner

        raise UnregisteredProcessorError(processor.name)

    @property
    def client(self) -> "Redis[bytes]":
        try:
            return self._client_var.get()
        except LookupError:
            client = Redis.from_url(**self._redis_config, decode_responses=False)
            self._client_var.set(client)
            return client

    async def start_flow(
        self,
        flow: "Flow",
        inputs: list["IO[Any]"] | None = None,
        all_outputs: bool = False,
    ) -> dict[str, "IO[Any]"]:
        return await self.executor.start(flow, inputs=inputs, all_outputs=all_outputs)

    async def load_flow_execution_plan(
        self, flow_execution_uuid: "UUID"
    ) -> "FlowExecutionPlan":
        plan: bytes | None = await self.client.get(f"flow:{flow_execution_uuid}")
        if plan is None:
            raise ValueError(
                f"Flow execution plan not found for UUID '{flow_execution_uuid}'."
            )

        return cast("FlowExecutionPlan", self.serializer.load(plan))

    def shutdown(self) -> None:
        self._portal_cm.__exit__(None, None, None)

    def __del__(self) -> None:
        self.shutdown()
