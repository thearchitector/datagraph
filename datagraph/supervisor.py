import warnings
from typing import TYPE_CHECKING, cast

from anyio.from_thread import start_blocking_portal
from anyio.lowlevel import RunVar

from .config import Config
from .exceptions import UnregisteredTaskError
from .redis.anyio import Redis
from .serialization import PicklingZstdSerializer

if TYPE_CHECKING:  # pragma: no cover
    from typing import Any, ClassVar
    from uuid import UUID

    from .executor import Executor
    from .flow import Flow
    from .flow_execution_plan import FlowExecutionPlan
    from .io import IO
    from .serialization import Serializer
    from .task import Task, TaskRunner


class Supervisor:
    _instance: "ClassVar[Supervisor | None]" = None
    _tasks: "ClassVar[dict[Task, TaskRunner]]" = {}

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
    def register_task(cls, task: "Task", runner: "TaskRunner") -> None:
        if task in cls._tasks:
            warnings.warn(
                f"Task '{task.name}' is already registered. This will override that"
                " implementation.",
                stacklevel=3,
            )

        cls._tasks[task] = runner

    @classmethod
    def get_task_runner(cls, task: "Task") -> "TaskRunner":
        if runner := cls._tasks.get(task):
            return runner

        raise UnregisteredTaskError(task.name)

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
