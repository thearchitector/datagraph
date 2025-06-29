from typing import Annotated

from annotated_types import Ge
from pydantic import PositiveInt
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    serialization_secret: str = "supersecretsecret"
    """Secret used for signing serialized data if using a supporting serializer."""

    flow_execution_advancement_timeout: Annotated[int, Ge(1)] = 5
    """ Max lock acquisition timeout in seconds for flow execution plan advancement."""

    io_read_timeout: PositiveInt = 1000
    """ Max iteration timeout in milliseconds for IO stream operations."""
    io_read_pending_timeout: PositiveInt = 30
    """ Max timeout in seconds to wait between successful IO stream operations."""
