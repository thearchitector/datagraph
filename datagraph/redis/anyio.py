from typing import TYPE_CHECKING

import sniffio
from redis.asyncio import Redis as _Redis
from redis.asyncio.client import Pipeline as _Pipeline
from redis.asyncio.lock import Lock as _Lock
from trio_asyncio import aio_as_trio

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable
    from typing import ParamSpec, TypeVar

    P = ParamSpec("P")
    T = TypeVar("T")


def anyize(coro_fn: "Callable[P, T]") -> "Callable[P, T]":
    if sniffio.current_async_library() == "trio":
        return aio_as_trio(coro_fn)
    else:
        return coro_fn


class Lock(_Lock):
    async def acquire(self, *args, **kwargs):
        return await anyize(super().acquire)(*args, **kwargs)


class Redis(_Redis):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def execute_command(self, *args, **options):
        return await anyize(super().execute_command)(*args, **options)

    def lock(self, *args, **kwargs) -> Lock:
        return super().lock(*args, lock_class=Lock, **kwargs)

    def pipeline(self, *args, **kwargs) -> "Pipeline":
        return Pipeline(*args, **kwargs)


class Pipeline(_Pipeline):
    async def execute(self, *args, **kwargs):
        return await anyize(super().execute)(*args, **kwargs)
