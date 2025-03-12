from typing import TYPE_CHECKING

import sniffio
from redis.asyncio import Redis as _Redis
from redis.asyncio.client import Pipeline as _Pipeline
from redis.asyncio.lock import Lock as _Lock
from trio_asyncio import aio_as_trio

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable
    from typing import Any, ParamSpec, TypeVar

    from redis.typing import KeyT

    P = ParamSpec("P")
    T = TypeVar("T")


def anyize(coro_fn: "Callable[P, T]") -> "Callable[P, T]":
    if sniffio.current_async_library() == "trio":
        return aio_as_trio(coro_fn)
    else:
        return coro_fn


class Lock(_Lock):
    async def acquire(
        self,
        blocking: bool | None = None,
        blocking_timeout: float | None = None,
        token: str | bytes | None = None,
    ) -> bool:
        return await anyize(super().acquire)(
            blocking=blocking, blocking_timeout=blocking_timeout, token=token
        )


class Redis(_Redis):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def execute_command(self, *args, **options) -> "Any":
        return await anyize(super().execute_command)(*args, **options)

    def lock(
        self,
        name: "KeyT",
        timeout: float | None = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: float | None = None,
        lock_class: type[_Lock] | None = None,
        thread_local: bool = True,
    ) -> _Lock:
        return super().lock(
            name,
            timeout,
            sleep,
            blocking,
            blocking_timeout,
            lock_class or Lock,
            thread_local,
        )

    def pipeline(
        self, transaction: bool = True, shard_hint: str | None = None
    ) -> "Pipeline":
        return Pipeline(
            self.connection_pool, self.response_callbacks, transaction, shard_hint
        )


class Pipeline(_Pipeline):
    async def execute(self, raise_on_error: bool = True) -> list["Any"]:
        return await anyize(super().execute)(raise_on_error)
