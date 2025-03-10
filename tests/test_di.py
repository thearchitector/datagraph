import pytest
from fast_depends import Depends

from datagraph import IO, IODef, Task


def _generate_b() -> str:
    return "b"


foo = Task(name="foo", inputs=[IODef(name="a")])


@foo
async def _foo(a: IO[str], b: str = Depends(_generate_b)):
    for i in range(3):
        yield IO(name="b", value=b * i)


@pytest.mark.anyio
async def test_execute_flow_simple():
    await foo.instance("hi")
