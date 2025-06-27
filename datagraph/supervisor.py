import atexit
import warnings
import weakref
from typing import TYPE_CHECKING, cast

from anyio.from_thread import start_blocking_portal
from anyio.lowlevel import RunVar
from anyio_atexit import run_finally
from glide import GlideClient, GlideClusterClient, GlideClusterClientConfiguration

from .config import Config
from .exceptions import UnregisteredProcessorError
from .serialization import PicklingZstdSerializer

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable
    from typing import Any, ClassVar
    from uuid import UUID

    from glide import GlideClientConfiguration, TGlideClient

    from .executor import Executor
    from .flow import Flow
    from .flow_execution_plan import FlowExecutionPlan
    from .io import IO, IOVal
    from .processor import Processor, ProcessorRunner
    from .serialization import Serializer


class Supervisor:
    _instance: "ClassVar[Supervisor | None]" = None
    _processors: "ClassVar[dict[Processor, ProcessorRunner]]" = {}

    def __init__(
        self,
        executor: "Executor",
        glide_config: "GlideClientConfiguration | GlideClusterClientConfiguration",
        serializer: "Serializer | None" = None,
        async_config: dict[str, "Any"] | None = None,
        **settings: "Any",
    ) -> None:
        if Supervisor._instance is not None:
            raise RuntimeError("Supervisor is already attached.")

        if async_config is None:
            async_config = {}

        self.config = Config(**settings)

        self._glide_config = glide_config
        self._client_var: RunVar["TGlideClient"] = RunVar("_client_var")

        self.executor: "Executor" = executor
        self.serializer: "Serializer" = serializer or PicklingZstdSerializer(
            self.config.serialization_secret
        )

        self._portal_cm = start_blocking_portal(
            async_config.pop("backend", "asyncio"), async_config
        )
        self.async_portal = self._portal_cm.__enter__()

        self._is_shutdown = False
        weakref.finalize(self, Supervisor._shutdown, weakref.ref(self))
        atexit.register(Supervisor._shutdown, weakref.ref(self))

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
        glide_config: "GlideClientConfiguration | GlideClusterClientConfiguration",
        serializer: "Serializer | None" = None,
        async_config: dict[str, "Any"] | None = None,
        **settings: "Any",
    ) -> "Supervisor":
        instance = cls(
            executor,
            glide_config,
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

    async def new_glide_client(self) -> "TGlideClient":
        return await (
            GlideClusterClient
            if isinstance(self._glide_config, GlideClusterClientConfiguration)
            else GlideClient
        ).create(self._glide_config)

    async def client(self) -> "TGlideClient":
        try:
            return self._client_var.get()
        except LookupError:
            client = await self.new_glide_client()
            run_finally(client.close)

            self._client_var.set(client)
            return client

    async def start_flow(
        self,
        flow: "Flow",
        inputs: list["IOVal[Any]"] | None = None,
        all_outputs: bool = False,
    ) -> dict[str, "IO[Any]"]:
        return await self.executor.start(flow, inputs=inputs, all_outputs=all_outputs)

    async def load_flow_execution_plan(
        self, flow_execution_uuid: "UUID"
    ) -> "FlowExecutionPlan":
        plan: bytes | None = await (await self.client()).get(
            f"flow:{flow_execution_uuid}"
        )
        if plan is None:
            raise ValueError(
                f"Flow execution plan not found for UUID '{flow_execution_uuid}'."
            )

        return cast("FlowExecutionPlan", self.serializer.load(plan))

    @staticmethod
    def _shutdown(instance_ref: "Callable[[], Supervisor | None]") -> None:
        # static using a weakref to prevent reference cycles
        if (instance := instance_ref()) and not instance._is_shutdown:
            try:
                instance._portal_cm.__exit__(None, None, None)
            except Exception as e:
                warnings.warn(
                    f"An exception occurred while shutting down the Supervisor: {e}",
                    stacklevel=2,
                )

            instance._is_shutdown = True

    def shutdown(self) -> None:
        Supervisor._shutdown(lambda: self)
