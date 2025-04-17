import pytest


@pytest.fixture
def celery_worker_parameters():
    return {"shutdown_timeout": 30.0}
