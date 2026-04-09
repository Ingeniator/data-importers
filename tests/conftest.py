import base64

import pytest
from fastapi.testclient import TestClient

from dataimporter.main import app


@pytest.fixture
def client() -> TestClient:
    yield TestClient(app)


@pytest.fixture
def auth_headers() -> dict[str, str]:
    creds = base64.b64encode(b"pk-test:sk-test").decode()
    return {"Authorization": f"Basic {creds}"}
