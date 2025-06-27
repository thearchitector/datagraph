from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Generic, TypeVar, cast

import anyio
from anyio import current_time
from asyncstdlib import zip as azip
from asyncstdlib import zip_longest as azip_longest
from glide import StreamReadOptions
from pydantic import RootModel, computed_field

from .exceptions import IOStreamTimeout, MismatchedIOError, ReadOnlyIOError
from .supervisor import Supervisor

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from typing import Any, TypeVarTuple

    from anyio.streams.memory import MemoryObjectSendStream

    from .flow_execution_plan import FlowExecutionPlan
    from .serialization import Serializer

    U = TypeVar("U")
    Us = TypeVarTuple("Us")

T = TypeVar("T")

_NOPAD = object()
RawStreamEntry = dict[bytes, dict[bytes, list[tuple[bytes, bytes]]]]


@dataclass(kw_only=True, unsafe_hash=True, slots=True)
class IOVal(Generic[T]):
    name: str
    value: T


class StreamCursor(Enum):
    FROM_FIRST = "0"
    FROM_LATEST = "+"
    ONLY_NEW = "$"


class StreamEntry(RootModel[RawStreamEntry]):
    @computed_field
    @property
    def stream_key(self) -> bytes:
        return next(iter(self.root))

    @computed_field  # type: ignore[misc]
    @property
    def entry_id(self) -> str:
        return next(iter(self.root[self.stream_key]))

    @computed_field  # type: ignore[misc]
    @property
    def data(self) -> bytes:
        return self.root[self.stream_key][self.entry_id][0][1]


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
        self._completion_value = f"flow:{flow_execution_plan.uuid}:io:{name}".encode()
        self._serializer: "Serializer" = Supervisor.instance().serializer
        self._read_only = read_only

    async def stream(
        self, cursor: StreamCursor = StreamCursor.FROM_FIRST
    ) -> "AsyncIterator[T]":
        # GLIDE client connections are not pooled. using a single client/connection
        # across every blocking stream would cause interference, since 1 stream
        # would block the client from processing other streams
        client = await Supervisor.instance().new_glide_client()

        try:
            last_entry_id: str = cursor.value
            last_stream_read: float = current_time()

            while True:
                raw_message: RawStreamEntry | None = await client.xread(
                    {self._stream_key: last_entry_id},
                    options=StreamReadOptions(
                        block_ms=Supervisor.instance().config.io_read_timeout,
                        count=1,
                    ),
                )

                # no message in the blocking window
                if not raw_message:
                    if (
                        current_time() - last_stream_read
                        > Supervisor.instance().config.io_read_pending_timeout
                    ):
                        raise IOStreamTimeout(
                            self.name,
                            Supervisor.instance().config.io_read_pending_timeout,
                        )

                    continue

                # if the message is the completion message, we're done
                message = StreamEntry(raw_message)
                if message.data == self._completion_value:
                    break

                ioval = cast(IOVal["T"], self._serializer.load(message.data))
                if ioval.name != self.name:
                    raise MismatchedIOError("read", ioval.name, self.name)

                last_entry_id = message.entry_id
                last_stream_read = current_time()
                yield ioval.value
        finally:
            await client.close()

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

        client = await Supervisor.instance().client()
        await client.xadd(self._stream_key, [("ioval", self._serializer.dump(value))])

    async def complete(self) -> None:
        client = await Supervisor.instance().client()
        await client.xadd(self._stream_key, [("eos", self._completion_value)])

    async def is_complete(self) -> bool:
        client = await Supervisor.instance().client()
        raw_message: RawStreamEntry | None = await client.xread(
            {self._stream_key: StreamCursor.FROM_LATEST.value},
            options=StreamReadOptions(count=1),
        )

        if not raw_message:
            return False

        message = StreamEntry(raw_message)
        return message.data == self._completion_value
