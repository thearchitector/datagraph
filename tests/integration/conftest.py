import pytest


@pytest.fixture(scope="session")
def celery_worker_parameters():
    return {"shutdown_timeout": 5.0}
