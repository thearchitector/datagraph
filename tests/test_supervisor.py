from unittest.mock import MagicMock

import pytest

from datagraph.supervisor import Supervisor


@pytest.mark.anyio
async def test_unattached_supervisor(monkeypatch):
    """Test that accessing Supervisor.instance() raises an error if not attached."""
    monkeypatch.setattr(Supervisor, "_instance", None)

    with pytest.raises(RuntimeError, match="Supervisor is not available"):
        Supervisor.instance()


@pytest.mark.anyio
async def test_attach_supervisor(monkeypatch):
    """Test that the attach method sets the instance correctly."""
    monkeypatch.setattr(Supervisor, "_instance", None)

    supervisor = Supervisor.attach(glide_config=MagicMock(), executor=MagicMock())

    assert Supervisor._instance is supervisor
    assert Supervisor.instance() is supervisor
    supervisor.shutdown()
