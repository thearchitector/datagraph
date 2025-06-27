import random
import uuid
from typing import TYPE_CHECKING

import anyio
from glide import ConditionalChange, ExpirySet, ExpiryType, Script

if TYPE_CHECKING:  # pragma: no cover
    from types import TracebackType

    from glide import TGlideClient


class LockError(Exception):
    def __init__(self, name: str, blocking_timeout: float) -> None:
        super().__init__(
            f"Could not acquire lock {name} within {blocking_timeout} seconds."
        )


LUA_RELEASE_SCRIPT = Script(
    """
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("del",KEYS[1])
    else
        return 0
    end
""".strip()
)


class ValkeyLock:
    def __init__(
        self,
        client: "TGlideClient",
        name: str,
        blocking_timeout: float,
        timeout: int = 10000,
    ):
        self.client = client
        self.name = name
        self.timeout = timeout
        self.blocking_timeout = blocking_timeout
        self.lock_value = str(uuid.uuid4())
        self._locked = False

    async def __aenter__(self) -> None:
        if self._locked:
            return

        start_time = anyio.current_time()

        while True:
            acquired = await self.client.set(
                self.name,
                self.lock_value,
                conditional_set=ConditionalChange.ONLY_IF_DOES_NOT_EXIST,
                expiry=ExpirySet(ExpiryType.SEC, self.timeout),
            )

            if acquired:
                self._locked = True
                return

            if anyio.current_time() - start_time > self.blocking_timeout:
                raise LockError(self.name, self.blocking_timeout)

            # NOTE: is this ok?
            await anyio.sleep(random.uniform(0.1, 0.4))

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: "TracebackType | None",
    ) -> None:
        if not self._locked:
            return

        await self.client.invoke_script(
            LUA_RELEASE_SCRIPT, keys=[self.name], args=[self.lock_value]
        )
        self._locked = False
