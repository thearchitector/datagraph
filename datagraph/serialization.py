import pickle
import pickletools
from abc import ABC, abstractmethod
from hashlib import blake2b
from hmac import compare_digest
from threading import local as thread_context
from typing import TYPE_CHECKING, final

from zstandard import ZstdCompressor, ZstdDecompressor

from .exceptions import TamperedDataError, UnserializableValueError

if TYPE_CHECKING:  # pragma: no cover
    from typing import Any, ClassVar


class Serializer(ABC):
    @abstractmethod
    async def serialize(self, value: "Any") -> bytes:
        """Serialize a value to compressed bytestream for storage."""
        raise NotImplementedError()

    @abstractmethod
    async def deserialize(self, data: bytes) -> "Any":
        """Deserialize a compressed bytestream from storage."""
        raise NotImplementedError()

    @abstractmethod
    async def compress(self, data: bytes) -> bytes:
        """Compress a bytestream for storage."""
        raise NotImplementedError()

    @abstractmethod
    async def decompress(self, data: bytes) -> bytes:
        """Decompress a bytestream from storage."""
        raise NotImplementedError()

    @final
    def dump(self, value: "Any") -> bytes:
        """Serialize and compress a value into a bytestream for storage."""
        try:
            return self.compress(self.serialize(value))
        except pickle.PickleError as e:
            raise UnserializableValueError(value) from e

    @final
    def load(self, data: bytes) -> "Any":
        """Deserialize and decompress a bytestream from storage."""
        return self.deserialize(self.decompress(data))


class PicklingZstdSerializer(Serializer):
    # Zstd is not thread safe so we should ensure a unique instance per thread
    _thread_context: "ClassVar[thread_context]" = thread_context()

    def __init__(self, secret: str) -> None:
        super().__init__()
        self.secret_key: bytes = secret.encode()

    def serialize(self, data: "Any") -> bytes:
        return pickletools.optimize(pickle.dumps(data, protocol=5))

    def deserialize(self, data: bytes) -> "Any":
        return pickle.loads(data)

    @property
    def compressor(self) -> "ZstdCompressor":
        if not hasattr(self._thread_context, "compressor"):
            self._thread_context.compressor = ZstdCompressor()

        return self._thread_context.compressor

    @property
    def decompressor(self) -> "ZstdDecompressor":
        if not hasattr(self._thread_context, "decompressor"):
            self._thread_context.decompressor = ZstdDecompressor()

        return self._thread_context.decompressor

    def compress(self, data: bytes) -> bytes:
        compressed = self.compressor.compress(data)
        signer = blake2b(digest_size=16, key=self.secret_key, usedforsecurity=True)
        signer.update(compressed)
        return signer.hexdigest().encode() + b"|" + compressed

    def decompress(self, compressed: bytes) -> bytes:
        try:
            signature, compressed = compressed.split(b"|", 1)
            signer = blake2b(digest_size=16, key=self.secret_key, usedforsecurity=True)
            signer.update(compressed)
            assert compare_digest(signer.hexdigest().encode(), signature)
        except (ValueError, AssertionError) as e:
            raise TamperedDataError() from e

        return self.decompressor.decompress(compressed)
