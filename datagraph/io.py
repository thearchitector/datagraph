from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

from asyncstdlib import zip as azip
from asyncstdlib import zip_longest as azip_longest
from pydantic import RootModel, computed_field
from redis.asyncio import RedisError

from .exceptions import MismatchedIOError, ReadOnlyIOError
from .supervisor import Supervisor

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from typing import Any, TypeVarTuple

    from redis.asyncio import Redis

    from .flow import FlowExecutionPlan
    from .serialization import Serializer

    U = TypeVar("U")
    Us = TypeVarTuple("Us")

T = TypeVar("T")

_NOPAD = object()
RawStreamEntry = list[tuple[str, list[tuple[str, dict[str, bytes]]]]]


@dataclass(kw_only=True, unsafe_hash=True, slots=True)
class IOVal(Generic[T]):
    output: str
    value: T


class StreamEntry(RootModel[RawStreamEntry]):
    @computed_field  # type: ignore[misc]
    @property
    def ioval_bytes(self) -> bytes:
        return self.root[0][1][0][1]["ioval"]

    @computed_field  # type: ignore[misc]
    @property
    def entry_id(self) -> str:
        return self.root[0][1][0][0]


class IO(Generic[T]):
    """
    Represents an input/output stream in the dataflow graph.
    Acts as a "pointer" to data in a shared store.
    """

    def __init__(
        self, name: str, flow_execution_plan: "FlowExecutionPlan", read_only: bool
    ) -> None:
        self.name = name
        self._stream_key = f"flow:{flow_execution_plan.uuid}:io:{name}:data"
        self._completion_key = f"flow:{flow_execution_plan.uuid}:io:{name}:complete"
        self._client: "Redis[bytes]" = Supervisor.instance.client
        self._serializer: "Serializer" = Supervisor.instance.serializer
        self._read_only = read_only

    async def stream(self) -> "AsyncIterator[T]":
        last_entry_id: str = "0"

        try:
            while True:
                raw_message: RawStreamEntry | None = await self._client.xread(
                    streams={self._stream_key: last_entry_id},
                    count=1,
                    block=Supervisor.instance.config.io_read_timeout,
                )

                # no message in the blocking window and the stream is complete, so
                # there's a pretty solid chance we won't ever stream more info
                if not raw_message:
                    if await self._client.get(self._completion_key):
                        break

                    continue

                message = StreamEntry(raw_message)
                ioval = self._serializer.load(message.ioval_bytes)

                if ioval.output != self.name:
                    raise MismatchedIOError("read", ioval.output, self.name)

                last_entry_id = message.entry_id
                yield ioval
        except RedisError as e:
            raise e

    async def first(self) -> T:
        return await anext(self.stream())

    async def stream_with(
        self, *others: "IO[U]", pad: "Any" = _NOPAD, strict_when_no_pad: bool = False
    ) -> "AsyncIterator[tuple[T, *Us]]":
        if pad is _NOPAD:
            zipper = azip
            zip_args = {"strict": strict_when_no_pad}
        else:
            zipper = azip_longest
            zip_args = {"fillvalue": pad}

        async for values in zipper(
            self.stream(), *(other.stream() for other in others), **zip_args
        ):
            yield values

    async def write(self, value: IOVal["T"]) -> None:
        if self._read_only:
            raise ReadOnlyIOError(self.name)
        elif value.output != self.name:
            raise MismatchedIOError("write", self.name, value.output)

        await self._client.xadd(
            self._stream_key, {"ioval": self._serializer.dump(value)}
        )

    async def complete(self) -> None:
        await self._client.set(self._completion_key, True)
