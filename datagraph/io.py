from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Generic, TypeVar, cast

import anyio
from anyio import current_time
from asyncstdlib import zip as azip
from asyncstdlib import zip_longest as azip_longest
from pydantic import RootModel, computed_field
from redis.asyncio import RedisError

from .exceptions import IOStreamTimeout, MismatchedIOError, ReadOnlyIOError
from .supervisor import Supervisor

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from typing import Any, TypeVarTuple

    from anyio.streams.memory import MemoryObjectSendStream
    from redis.asyncio import Redis

    from .flow_execution_plan import FlowExecutionPlan
    from .serialization import Serializer

    U = TypeVar("U")
    Us = TypeVarTuple("Us")

T = TypeVar("T")

_NOPAD = object()
RawStreamEntry = list[tuple[str, list[tuple[str, dict[str, bytes]]]]]


@dataclass(kw_only=True, unsafe_hash=True, slots=True)
class IOVal(Generic[T]):
    name: str
    value: T


class StreamCursor(Enum):
    FROM_FIRST = "0"
    FROM_LATEST = "+"
    ONLY_NEW = "$"


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
        self._client: "Redis[bytes]" = Supervisor.instance().client
        self._serializer: "Serializer" = Supervisor.instance().serializer
        self._read_only = read_only

    async def stream(
        self, cursor: StreamCursor = StreamCursor.FROM_FIRST
    ) -> "AsyncIterator[T]":
        last_entry_id: str = cursor.value
        last_stream_read: float = current_time()

        try:
            while True:
                raw_message: RawStreamEntry | None = await self._client.xread(
                    streams={self._stream_key: last_entry_id},
                    count=1,
                    block=Supervisor.instance().config.io_read_timeout,
                )

                # no message in the blocking window and the stream is complete, so
                # there's a pretty solid chance we won't ever stream more info
                if not raw_message:
                    if await self.is_complete():
                        break
                    elif (
                        current_time() - last_stream_read
                        > Supervisor.instance().config.io_read_pending_timeout
                    ):
                        raise IOStreamTimeout(
                            self.name,
                            Supervisor.instance().config.io_read_pending_timeout,
                        )

                    continue

                message = StreamEntry(raw_message)
                ioval = cast(IOVal["T"], self._serializer.load(message.ioval_bytes))

                if ioval.name != self.name:
                    raise MismatchedIOError("read", ioval.name, self.name)

                last_entry_id = message.entry_id
                last_stream_read = current_time()
                yield ioval.value
        except RedisError as e:
            raise e

    async def first(self) -> T:
        return await anext(self.stream(cursor=StreamCursor.FROM_FIRST))  # noqa: F821

    # async def latest(self) -> T:
    #     return await anext(self.stream(cursor=StreamCursor.FROM_LATEST))

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

    async def as_available(self, *others: "IO[U]") -> "AsyncIterator[T | U]":
        send_stream, receive_stream = anyio.create_memory_object_stream["T | U"]()

        async def _read_stream(
            io_obj: "IO[T | U]", send_stream: "MemoryObjectSendStream[T | U]"
        ):
            async with send_stream:
                async for value in io_obj.stream():
                    await send_stream.send(value)

        async with anyio.create_task_group() as tg:
            for io in (self, *others):
                tg.start_soon(_read_stream, io, send_stream)

            async with receive_stream:
                async for value in receive_stream:
                    yield value

    async def write(self, value: "T") -> None:
        if self._read_only:
            raise ReadOnlyIOError(self.name)
        elif value.name != self.name:
            raise MismatchedIOError("write", self.name, value.name)

        await self._client.xadd(
            self._stream_key, {"ioval": self._serializer.dump(value)}
        )

    async def complete(self) -> None:
        await self._client.set(self._completion_key, b"true")

    async def is_complete(self) -> bool:
        return await self._client.get(self._completion_key) == b"true"
