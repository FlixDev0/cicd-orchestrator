"""
Tests de la API REST.
Usan el TestClient de FastAPI — no necesitan servidor corriendo.
Ejecutar con: pytest tests/test_api.py -v
"""

from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from fastapi.testclient import TestClient
from orchestrator.api.app import app
from orchestrator.api.store import store
from orchestrator.core.models import JobResult, PipelineRun, RunStatus
from orchestrator.storage.database import init_db, get_engine, Base

VALID_YAML = """
name: test-pipeline
description: Pipeline de prueba

jobs:
  build:
    image: python:3.11-slim
    steps:
      - name: Compilar
        run: echo ok

  test:
    image: python:3.11-slim
    needs: [build]
    steps:
      - name: Tests
        run: pytest
"""

INVALID_YAML = """
name: pipeline-roto
jobs: {}
"""


@pytest.fixture(autouse=True)
def use_memory_db(tmp_path):
    import orchestrator.storage.database as db_module
    from orchestrator.storage.database import create_db_engine
    from sqlalchemy.orm import sessionmaker

    db_url = f"sqlite:///{tmp_path}/test.db"
    test_engine = create_db_engine(db_url)
    Base.metadata.create_all(test_engine)

    db_module._engine = test_engine
    db_module._SessionFactory = sessionmaker(
        bind=test_engine, autocommit=False, autoflush=False
    )

    import orchestrator.storage.db_store as ds
    ds.store = ds.DatabaseStore()

    yield

    db_module._engine = None
    db_module._SessionFactory = None


@pytest.fixture
def client():
    from orchestrator.api.app import app
    return TestClient(app)

# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_ok(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# Pipelines
# ---------------------------------------------------------------------------

class TestCreatePipeline:

    def test_crear_pipeline_valido(self, client):
        r = client.post("/pipelines", json={"yaml_content": VALID_YAML})
        assert r.status_code == 201
        data = r.json()
        assert "id" in data
        assert data["name"] == "test-pipeline"
        assert data["description"] == "Pipeline de prueba"

    def test_yaml_invalido_retorna_422(self, client):
        r = client.post("/pipelines", json={"yaml_content": INVALID_YAML})
        assert r.status_code == 422

    def test_yaml_vacio_retorna_422(self, client):
        r = client.post("/pipelines", json={"yaml_content": "x"})
        assert r.status_code == 422

    def test_body_sin_yaml_retorna_422(self, client):
        r = client.post("/pipelines", json={})
        assert r.status_code == 422


class TestListPipelines:

    def test_lista_vacia_inicial(self, client):
        r = client.get("/pipelines")
        assert r.status_code == 200
        assert r.json() == []

    def test_lista_con_pipelines(self, client):
        client.post("/pipelines", json={"yaml_content": VALID_YAML})
        client.post("/pipelines", json={"yaml_content": VALID_YAML})
        r = client.get("/pipelines")
        assert len(r.json()) == 2


class TestGetPipeline:

    def test_obtener_pipeline_existente(self, client):
        created = client.post("/pipelines", json={"yaml_content": VALID_YAML}).json()
        r = client.get(f"/pipelines/{created['id']}")
        assert r.status_code == 200
        assert r.json()["id"] == created["id"]

    def test_pipeline_inexistente_retorna_404(self, client):
        r = client.get("/pipelines/no-existe")
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------

class TestTriggerRun:

    def test_disparar_run_retorna_202(self, client):
        pipeline = client.post("/pipelines", json={"yaml_content": VALID_YAML}).json()

        # Parchear _execute_run para que no corra Docker de verdad
        with patch("orchestrator.api.app._execute_run", new=AsyncMock()):
            r = client.post(
                f"/pipelines/{pipeline['id']}/runs",
                json={"branch": "main"},
            )

        assert r.status_code == 202
        data = r.json()
        assert "id" in data
        assert data["pipeline_id"] == pipeline["id"]
        assert data["branch"] == "main"
        assert data["status"] in ("pending", "running")

    def test_run_pipeline_inexistente_retorna_404(self, client):
        r = client.post("/pipelines/no-existe/runs", json={"branch": "main"})
        assert r.status_code == 404

    def test_run_con_trigger_manual(self, client):
        pipeline = client.post("/pipelines", json={"yaml_content": VALID_YAML}).json()

        with patch("orchestrator.api.app._execute_run", new=AsyncMock()):
            r = client.post(
                f"/pipelines/{pipeline['id']}/runs",
                json={"branch": "develop", "trigger": "manual"},
            )

        assert r.status_code == 202
        assert r.json()["trigger"] == "manual"


class TestGetRun:

    def _create_run(self, client) -> dict:
        pipeline = client.post("/pipelines", json={"yaml_content": VALID_YAML}).json()
        with patch("orchestrator.api.app._execute_run", new=AsyncMock()):
            return client.post(
                f"/pipelines/{pipeline['id']}/runs",
                json={"branch": "main"},
            ).json()

    def test_obtener_run_existente(self, client):
        run = self._create_run(client)
        r = client.get(f"/runs/{run['id']}")
        assert r.status_code == 200
        assert r.json()["id"] == run["id"]

    def test_run_inexistente_retorna_404(self, client):
        r = client.get("/runs/no-existe")
        assert r.status_code == 404


class TestListRuns:

    def test_listar_todos_los_runs(self, client):
        pipeline = client.post("/pipelines", json={"yaml_content": VALID_YAML}).json()
        with patch("orchestrator.api.app._execute_run", new=AsyncMock()):
            client.post(f"/pipelines/{pipeline['id']}/runs", json={"branch": "main"})
            client.post(f"/pipelines/{pipeline['id']}/runs", json={"branch": "main"})

        r = client.get("/runs")
        assert r.status_code == 200
        assert len(r.json()) == 2

    def test_filtrar_runs_por_pipeline(self, client):
        p1 = client.post("/pipelines", json={"yaml_content": VALID_YAML}).json()
        p2 = client.post("/pipelines", json={"yaml_content": VALID_YAML}).json()

        with patch("orchestrator.api.app._execute_run", new=AsyncMock()):
            client.post(f"/pipelines/{p1['id']}/runs", json={"branch": "main"})
            client.post(f"/pipelines/{p2['id']}/runs", json={"branch": "main"})

        r = client.get(f"/runs?pipeline_id={p1['id']}")
        assert len(r.json()) == 1
        assert r.json()[0]["pipeline_id"] == p1["id"]


# ---------------------------------------------------------------------------
# Schemas de respuesta
# ---------------------------------------------------------------------------

class TestRunResponseSchema:

    def test_run_response_tiene_campos_requeridos(self, client):
        pipeline = client.post("/pipelines", json={"yaml_content": VALID_YAML}).json()
        with patch("orchestrator.api.app._execute_run", new=AsyncMock()):
            run = client.post(
                f"/pipelines/{pipeline['id']}/runs",
                json={"branch": "main"},
            ).json()

        assert "id"          in run
        assert "pipeline_id" in run
        assert "status"      in run
        assert "trigger"     in run
        assert "branch"      in run
        assert "started_at"  in run
