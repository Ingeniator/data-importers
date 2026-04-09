from fastapi.testclient import TestClient

from dataimporter.config import Settings, get_settings
from dataimporter.main import app


def test_livez(client) -> None:
    resp = client.get("/livez")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_health_no_datasources():
    app.dependency_overrides[get_settings] = lambda: Settings()
    client = TestClient(app)
    resp = client.get("/health")
    app.dependency_overrides.clear()
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
