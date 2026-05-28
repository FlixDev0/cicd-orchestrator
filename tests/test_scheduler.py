"""
Tests del Scheduler.
Usan un DockerRunner mockeado para verificar la lógica de coordinación
sin necesitar Docker real.

Ejecutar con: pytest tests/test_scheduler.py -v
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.core.models import (
    JobResult,
    RunStatus,
    StepResult,
)
from orchestrator.core.parser import PipelineParser
from orchestrator.execution.docker_runner import DockerRunner
from orchestrator.execution.scheduler import Scheduler

PARSER = PipelineParser()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_runner(job_results: dict[str, RunStatus]) -> DockerRunner:
    """
    Crea un DockerRunner mockeado que retorna el RunStatus indicado
    para cada job_key.
    """
    runner = MagicMock(spec=DockerRunner)

    async def fake_run_job(job_key, job, env=None, on_log=None):
        status = job_results.get(job_key, RunStatus.SUCCESS)
        return JobResult(
            job_key=job_key,
            status=status,
            steps=[StepResult(step_name="step-0", status=status, exit_code=0 if status == RunStatus.SUCCESS else 1)],
        )

    runner.run_job = fake_run_job
    return runner


def make_pipeline(jobs_config: dict[str, list[str]]) -> object:
    """
    Crea un pipeline desde un dict {job_key: [needs]}.
    """
    jobs_yaml = ""
    for name, needs in jobs_config.items():
        needs_str = str(needs).replace("'", '"')
        jobs_yaml += f"""
  {name}:
    image: alpine
    needs: {needs_str}
    steps:
      - run: echo {name}
"""
    return PARSER.parse_string(f"name: test-pipeline\njobs:\n{jobs_yaml}")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSchedulerBasic:

    @pytest.mark.asyncio
    async def test_pipeline_simple_exitoso(self):
        pipeline = make_pipeline({"build": [], "test": ["build"]})
        runner   = make_runner({"build": RunStatus.SUCCESS, "test": RunStatus.SUCCESS})
        scheduler = Scheduler(runner=runner)

        run = await scheduler.execute(pipeline)

        assert run.status == RunStatus.SUCCESS
        assert run.jobs["build"].status == RunStatus.SUCCESS
        assert run.jobs["test"].status == RunStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_pipeline_un_job(self):
        pipeline  = make_pipeline({"lint": []})
        runner    = make_runner({"lint": RunStatus.SUCCESS})
        scheduler = Scheduler(runner=runner)

        run = await scheduler.execute(pipeline)

        assert run.status == RunStatus.SUCCESS
        assert "lint" in run.jobs

    @pytest.mark.asyncio
    async def test_run_tiene_timestamps(self):
        pipeline  = make_pipeline({"build": []})
        runner    = make_runner({"build": RunStatus.SUCCESS})
        scheduler = Scheduler(runner=runner)

        run = await scheduler.execute(pipeline)

        assert run.started_at is not None
        assert run.ended_at is not None
        assert run.ended_at >= run.started_at


class TestSchedulerFailurePropagation:

    @pytest.mark.asyncio
    async def test_fallo_upstream_salta_dependientes(self):
        """Si 'build' falla, 'test' y 'deploy' deben ser SKIPPED."""
        pipeline = make_pipeline({
            "build":  [],
            "test":   ["build"],
            "deploy": ["test"],
        })
        runner    = make_runner({"build": RunStatus.FAILED})
        scheduler = Scheduler(runner=runner)

        run = await scheduler.execute(pipeline)

        assert run.status == RunStatus.FAILED
        assert run.jobs["build"].status  == RunStatus.FAILED
        assert run.jobs["test"].status   == RunStatus.SKIPPED
        assert run.jobs["deploy"].status == RunStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_fallo_no_afecta_ramas_independientes(self):
        """Si 'lint' falla, 'build' (sin dependencia) sigue corriendo."""
        pipeline = make_pipeline({
            "lint":  [],
            "build": [],
            "test":  ["build"],   # test depende de build, no de lint
        })
        runner = make_runner({
            "lint":  RunStatus.FAILED,
            "build": RunStatus.SUCCESS,
            "test":  RunStatus.SUCCESS,
        })
        scheduler = Scheduler(runner=runner)

        run = await scheduler.execute(pipeline)

        assert run.jobs["lint"].status  == RunStatus.FAILED
        assert run.jobs["build"].status == RunStatus.SUCCESS
        assert run.jobs["test"].status  == RunStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_run_status_failed_si_algun_job_falla(self):
        pipeline  = make_pipeline({"a": [], "b": []})
        runner    = make_runner({"a": RunStatus.SUCCESS, "b": RunStatus.FAILED})
        scheduler = Scheduler(runner=runner)

        run = await scheduler.execute(pipeline)

        assert run.status == RunStatus.FAILED


class TestSchedulerParallelism:

    @pytest.mark.asyncio
    async def test_jobs_sin_dependencias_corren_en_paralelo(self):
        """
        Verifica que lint y build (sin dependencias) se lancen en paralelo
        comprobando que el tiempo total es menor que la suma individual.
        """
        pipeline = make_pipeline({"lint": [], "build": [], "test": ["lint", "build"]})

        execution_order: list[str] = []

        async def fake_run_job(job_key, job, env=None, on_log=None):
            execution_order.append(f"start:{job_key}")
            await asyncio.sleep(0.05)   # simula trabajo
            execution_order.append(f"end:{job_key}")
            return JobResult(job_key=job_key, status=RunStatus.SUCCESS)

        runner = MagicMock(spec=DockerRunner)
        runner.run_job = fake_run_job
        scheduler = Scheduler(runner=runner)

        run = await scheduler.execute(pipeline)

        # lint y build deben haber empezado antes de que cualquiera termine
        lint_start  = execution_order.index("start:lint")
        build_start = execution_order.index("start:build")
        lint_end    = execution_order.index("end:lint")
        build_end   = execution_order.index("end:build")

        # Ambos deben haber iniciado antes de que el primero termine
        first_end = min(lint_end, build_end)
        assert lint_start < first_end
        assert build_start < first_end

        # test debe correr después de ambos
        test_start = execution_order.index("start:test")
        assert test_start > lint_end
        assert test_start > build_end

    @pytest.mark.asyncio
    async def test_todos_los_jobs_se_ejecutan(self):
        pipeline = make_pipeline({
            "a": [],
            "b": [],
            "c": ["a"],
            "d": ["b"],
            "e": ["c", "d"],
        })
        runner    = make_runner({k: RunStatus.SUCCESS for k in "abcde"})
        scheduler = Scheduler(runner=runner)

        run = await scheduler.execute(pipeline)

        assert run.status == RunStatus.SUCCESS
        assert set(run.jobs.keys()) == set("abcde")


class TestSchedulerEvents:

    @pytest.mark.asyncio
    async def test_emite_eventos_run_y_jobs(self):
        pipeline = make_pipeline({"build": [], "test": ["build"]})
        runner   = make_runner({"build": RunStatus.SUCCESS, "test": RunStatus.SUCCESS})
        scheduler = Scheduler(runner=runner)

        events: list[str] = []
        await scheduler.execute(
            pipeline,
            on_event=lambda event, data: events.append(event),
        )

        assert "run_started"   in events
        assert "job_started"   in events
        assert "job_finished"  in events
        assert "run_finished"  in events

    @pytest.mark.asyncio
    async def test_evento_run_finished_tiene_status(self):
        pipeline = make_pipeline({"build": []})
        runner   = make_runner({"build": RunStatus.SUCCESS})
        scheduler = Scheduler(runner=runner)

        finish_data = {}

        def on_event(event, data):
            if event == "run_finished":
                finish_data.update(data)

        await scheduler.execute(pipeline, on_event=on_event)

        assert finish_data["status"] == RunStatus.SUCCESS.value
        assert "duration" in finish_data
        assert "jobs" in finish_data
