"""
Tests del DatabaseStore — verifica que la persistencia funciona correctamente.
Usa SQLite en archivo temporal para que no dejen archivos en el proyecto.
Ejecutar con: pytest tests/test_db_store.py -v
"""

import uuid
from datetime import datetime
import pytest
from orchestrator.core.models import (
    JobResult, PipelineRun, RunStatus, StepResult, TriggerType,
)
from orchestrator.core.parser import PipelineParser
from orchestrator.storage.database import Base, create_db_engine, init_db
from orchestrator.storage.db_store import DatabaseStore, NotFoundError

PARSER = PipelineParser()

SAMPLE_YAML = """
name: store-test-pipeline
description: "Pipeline para tests del store"
jobs:
  build:
    image: python:3.11-slim
    steps:
      - run: echo build
  test:
    image: python:3.11-slim
    needs: [build]
    steps:
      - run: pytest
"""


@pytest.fixture
def store(tmp_path):
    """Store con DB SQLite temporal, limpia por test."""
    db_url = f"sqlite:///{tmp_path}/test_store.db"
    import orchestrator.storage.database as db_module
    from sqlalchemy.orm import sessionmaker

    engine = create_db_engine(db_url)
    Base.metadata.create_all(engine)
    db_module._engine         = engine
    db_module._SessionFactory = sessionmaker(
        bind=engine, autocommit=False, autoflush=False
    )
    yield DatabaseStore()
    db_module._engine         = None
    db_module._SessionFactory = None


# Helper: crea un pipeline en el store y retorna su ID
def create_pipeline(store) -> str:
    pid = str(uuid.uuid4())
    store.save_pipeline(pid, PARSER.parse_string(SAMPLE_YAML), SAMPLE_YAML)
    return pid


# Helper: construye un PipelineRun para un pipeline_id existente
def make_run(pipeline_id: str, status=RunStatus.RUNNING) -> PipelineRun:
    return PipelineRun(
        pipeline_id=uuid.UUID(pipeline_id),
        trigger=TriggerType.MANUAL,
        branch="main",
        status=status,
    )


# ---------------------------------------------------------------------------
# Tests de Pipelines
# ---------------------------------------------------------------------------

class TestPipelineStore:

    def test_guardar_y_recuperar_pipeline(self, store):
        pid    = create_pipeline(store)
        record = store.get_pipeline(pid)
        assert record["id"]   == pid
        assert record["name"] == "store-test-pipeline"
        assert "definition"   in record

    def test_pipeline_inexistente_lanza_not_found(self, store):
        with pytest.raises(NotFoundError):
            store.get_pipeline("no-existe")

    def test_listar_pipelines_vacio(self, store):
        assert store.list_pipelines() == []

    def test_listar_multiples_pipelines(self, store):
        create_pipeline(store)
        create_pipeline(store)
        assert len(store.list_pipelines()) == 2

    def test_actualizar_pipeline_existente(self, store):
        pid    = create_pipeline(store)
        yaml_v2 = SAMPLE_YAML.replace("store-test-pipeline", "pipeline-v2")
        store.save_pipeline(pid, PARSER.parse_string(yaml_v2), yaml_v2)

        assert store.get_pipeline(pid)["name"] == "pipeline-v2"
        assert len(store.list_pipelines()) == 1   # upsert, no duplicado


# ---------------------------------------------------------------------------
# Tests de Runs
# ---------------------------------------------------------------------------

class TestRunStore:

    def test_guardar_y_recuperar_run(self, store):
        pid = create_pipeline(store)
        run = make_run(pid)
        store.save_run(run)

        recovered = store.get_run(str(run.id))
        assert str(recovered.id)          == str(run.id)
        assert str(recovered.pipeline_id) == pid
        assert recovered.status           == RunStatus.RUNNING
        assert recovered.branch           == "main"

    def test_run_inexistente_lanza_not_found(self, store):
        with pytest.raises(NotFoundError):
            store.get_run("no-existe")

    def test_actualizar_estado_run(self, store):
        pid = create_pipeline(store)
        run = make_run(pid)
        store.save_run(run)

        run.status   = RunStatus.SUCCESS
        run.ended_at = datetime.utcnow()
        store.save_run(run)

        recovered = store.get_run(str(run.id))
        assert recovered.status   == RunStatus.SUCCESS
        assert recovered.ended_at is not None

    def test_guardar_run_con_jobs_y_steps(self, store):
        pid = create_pipeline(store)
        run = make_run(pid)
        run.jobs["build"] = JobResult(
            job_key="build",
            status=RunStatus.SUCCESS,
            ended_at=datetime.utcnow(),
            steps=[StepResult(step_name="Compilar", status=RunStatus.SUCCESS, exit_code=0)],
        )
        store.save_run(run)

        recovered = store.get_run(str(run.id))
        assert "build"                          in recovered.jobs
        assert recovered.jobs["build"].status   == RunStatus.SUCCESS
        assert len(recovered.jobs["build"].steps) == 1
        assert recovered.jobs["build"].steps[0].exit_code == 0

    def test_listar_runs_vacio(self, store):
        assert store.list_runs() == []

    def test_listar_todos_los_runs(self, store):
        pid = create_pipeline(store)
        store.save_run(make_run(pid))
        store.save_run(make_run(pid))
        assert len(store.list_runs()) == 2

    def test_filtrar_runs_por_pipeline(self, store):
        pid1 = create_pipeline(store)
        pid2 = create_pipeline(store)
        store.save_run(make_run(pid1))
        store.save_run(make_run(pid1))
        store.save_run(make_run(pid2))

        assert len(store.list_runs(pipeline_id=pid1)) == 2
        assert len(store.list_runs(pipeline_id=pid2)) == 1

    def test_run_con_job_fallido(self, store):
        pid = create_pipeline(store)
        run = make_run(pid, status=RunStatus.FAILED)
        run.jobs["test"] = JobResult(
            job_key="test",
            status=RunStatus.FAILED,
            ended_at=datetime.utcnow(),
            steps=[StepResult(step_name="pytest", status=RunStatus.FAILED, exit_code=1)],
        )
        store.save_run(run)

        recovered = store.get_run(str(run.id))
        assert recovered.status                         == RunStatus.FAILED
        assert recovered.jobs["test"].steps[0].exit_code == 1
