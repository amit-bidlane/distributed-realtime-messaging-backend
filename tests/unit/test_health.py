from fastapi.testclient import TestClient

from app.main import create_app


class Ready:
    async def check(self) -> bool:
        return True


class NotReady:
    async def check(self) -> bool:
        return False


def test_health_returns_ok() -> None:
    with TestClient(create_app()) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_reports_dependency_availability() -> None:
    app = create_app()
    app.state.readiness = Ready()

    with TestClient(app) as client:
        response = client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_ready_returns_service_unavailable_when_dependency_is_down() -> None:
    app = create_app()
    app.state.readiness = NotReady()

    with TestClient(app) as client:
        response = client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {"detail": "not ready"}
