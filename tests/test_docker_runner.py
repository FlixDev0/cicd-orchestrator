"""
Tests del Docker Runner.
Usan un mock del cliente Docker para correr sin necesitar Docker real.
Ejecutar con: pytest tests/test_docker_runner.py -v
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.core.models import JobDefinition, RunStatus, StepDefinition
from orchestrator.execution.docker_runner import DockerRunner, DockerRunnerError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_job(steps_cmds: list[str], image: str = "python:3.11-slim") -> JobDefinition:
    """Crea un JobDefinition de prueba con los comandos dados."""
    return JobDefinition(
        name="test-job",
        image=image,
        steps=[
            StepDefinition(name=f"step-{i}", run=cmd, timeout=30)
            for i, cmd in enumerate(steps_cmds)
        ],
    )


def make_mock_container(exit_codes: list[int]) -> MagicMock:
    """
    Crea un mock de Container que simula exec_create/exec_start/exec_inspect
    para una lista de exit_codes (uno por step).
    """
    container = MagicMock()
    container.id = "fake-container-id"

    call_count = [0]

    def fake_exec_inspect(exec_id):
        idx = min(call_count[0] - 1, len(exit_codes) - 1)
        return {"ExitCode": exit_codes[idx]}

    def fake_exec_start(exec_id, stream=False, demux=False):
        call_count[0] += 1
        return iter([b"output line\n"])

    container.client.api.exec_create.return_value = {"Id": "fake-exec-id"}
    container.client.api.exec_start.side_effect = fake_exec_start
    container.client.api.exec_inspect.side_effect = fake_exec_inspect
    return container


# ---------------------------------------------------------------------------
# Helper para parchear DockerRunner sin Docker real
# ---------------------------------------------------------------------------

def patched_runner() -> DockerRunner:
    """Crea un DockerRunner con el cliente Docker completamente mockeado."""
    with patch("docker.from_env") as mock_docker:
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_docker.return_value = mock_client
        runner = DockerRunner()
        return runner


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDockerRunnerInit:

    def test_falla_si_docker_no_disponible(self):
        with patch("docker.from_env") as mock_docker:
            mock_docker.side_effect = Exception("Docker not running")
            with pytest.raises(DockerRunnerError, match="Docker"):
                DockerRunner()

    def test_inicializa_correctamente_con_docker_disponible(self):
        runner = patched_runner()
        assert runner is not None


class TestDockerRunnerJobExecution:

    @pytest.mark.asyncio
    async def test_job_exitoso_un_step(self):
        runner = patched_runner()
        job = make_job(["echo hello"])
        container = make_mock_container(exit_codes=[0])

        logs = []

        with (
            patch.object(runner, "_pull_image", new=AsyncMock()),
            patch.object(runner, "_create_container", new=AsyncMock(return_value=container)),
            patch.object(runner, "_cleanup", new=AsyncMock()),
        ):
            result = await runner.run_job(
                job_key="build",
                job=job,
                on_log=lambda k, s, l: logs.append(l),
            )

        assert result.status == RunStatus.SUCCESS
        assert result.job_key == "build"
        assert len(result.steps) == 1
        assert result.steps[0].status == RunStatus.SUCCESS
        assert result.steps[0].exit_code == 0

    @pytest.mark.asyncio
    async def test_job_falla_en_primer_step_salta_restantes(self):
        runner = patched_runner()
        job = make_job(["exit 1", "echo ignorado", "echo ignorado2"])
        container = make_mock_container(exit_codes=[1])

        with (
            patch.object(runner, "_pull_image", new=AsyncMock()),
            patch.object(runner, "_create_container", new=AsyncMock(return_value=container)),
            patch.object(runner, "_cleanup", new=AsyncMock()),
        ):
            result = await runner.run_job(job_key="test", job=job)

        assert result.status == RunStatus.FAILED
        assert result.steps[0].status == RunStatus.FAILED
        assert result.steps[1].status == RunStatus.SKIPPED
        assert result.steps[2].status == RunStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_job_multiples_steps_todos_exitosos(self):
        runner = patched_runner()
        job = make_job(["echo a", "echo b", "echo c"])
        container = make_mock_container(exit_codes=[0, 0, 0])

        with (
            patch.object(runner, "_pull_image", new=AsyncMock()),
            patch.object(runner, "_create_container", new=AsyncMock(return_value=container)),
            patch.object(runner, "_cleanup", new=AsyncMock()),
        ):
            result = await runner.run_job(job_key="lint", job=job)

        assert result.status == RunStatus.SUCCESS
        assert len(result.steps) == 3
        assert all(s.status == RunStatus.SUCCESS for s in result.steps)

    @pytest.mark.asyncio
    async def test_cleanup_se_llama_siempre_aunque_falle(self):
        runner = patched_runner()
        job = make_job(["exit 1"])
        container = make_mock_container(exit_codes=[1])
        cleanup_mock = AsyncMock()

        with (
            patch.object(runner, "_pull_image", new=AsyncMock()),
            patch.object(runner, "_create_container", new=AsyncMock(return_value=container)),
            patch.object(runner, "_cleanup", new=cleanup_mock),
        ):
            result = await runner.run_job(job_key="test", job=job)

        cleanup_mock.assert_called_once()
        assert result.status == RunStatus.FAILED

    @pytest.mark.asyncio
    async def test_log_callback_recibe_lineas(self):
        runner = patched_runner()
        job = make_job(["echo hello world"])
        container = make_mock_container(exit_codes=[0])

        received_logs: list[tuple] = []

        with (
            patch.object(runner, "_pull_image", new=AsyncMock()),
            patch.object(runner, "_create_container", new=AsyncMock(return_value=container)),
            patch.object(runner, "_cleanup", new=AsyncMock()),
        ):
            await runner.run_job(
                job_key="build",
                job=job,
                on_log=lambda k, s, l: received_logs.append((k, s, l)),
            )

        # Debe haber recibido al menos el comando y el output
        assert len(received_logs) > 0
        job_keys = [log[0] for log in received_logs]
        assert all(k == "build" for k in job_keys)

    @pytest.mark.asyncio
    async def test_result_tiene_timestamps(self):
        runner = patched_runner()
        job = make_job(["echo ok"])
        container = make_mock_container(exit_codes=[0])

        with (
            patch.object(runner, "_pull_image", new=AsyncMock()),
            patch.object(runner, "_create_container", new=AsyncMock(return_value=container)),
            patch.object(runner, "_cleanup", new=AsyncMock()),
        ):
            result = await runner.run_job(job_key="build", job=job)

        assert result.started_at is not None
        assert result.ended_at is not None
        assert result.ended_at >= result.started_at
        assert result.duration_seconds is not None
        assert result.duration_seconds >= 0
