from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic

from asyncstdlib import zip as azip
from asyncstdlib import zip_longest as azip_longest
from redis.asyncio import RedisError

from .exceptions import MismatchedIOError, ReadOnlyIOError
from .flow import FlowExecutionPlan
from .supervisor import Supervisor

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from typing import Any, TypeVar, TypeVarTuple

    from redis.asyncio import Redis

    T = TypeVar("T")
    U = TypeVar("U")
    Us = TypeVarTuple("Us")


_NOPAD = object()


@dataclass(kw_only=True, unsafe_hash=True, slots=True)
class IOVal:
    output: str
    value: "Any"


class IO(Generic["T"]):
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
        self._read_only = read_only

    async def stream(self) -> "AsyncIterator[T]":
        last_id: str = "0"

        try:
            while True:
                raw_message = await self._client.xread(
                    streams={self._stream_key: last_id},
                    count=1,
                    block=Supervisor.instance.config.io_read_timeout,
                )

                if not raw_message:
                    if await self._client.get(self._completion_key):
                        break

                    continue

                message = StreamMessage(raw_message)
                last_id = message.message_id
                yield message.signal
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
            self._stream_key, {"ioval": Supervisor.instance.serializer.dump(value)}
        )
