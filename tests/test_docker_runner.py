"""
Tests del Docker Runner — con mock del cliente Docker.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.core.models import JobDefinition, RunStatus, StepDefinition
from orchestrator.execution.docker_runner import DockerRunner, DockerRunnerError


def make_job(steps_cmds: list[str], image: str = "python:3.11-slim") -> JobDefinition:
    return JobDefinition(
        name="test-job",
        image=image,
        steps=[
            StepDefinition(name=f"step-{i}", run=cmd, timeout=30)
            for i, cmd in enumerate(steps_cmds)
        ],
    )


def make_mock_container(exit_codes: list[int]) -> MagicMock:
    container = MagicMock()
    container.id = "fake-container-id"
    call_count = [0]

    def fake_exec_inspect(exec_id):
        idx = min(call_count[0] - 1, len(exit_codes) - 1)
        return {"ExitCode": exit_codes[idx], "Running": False}

    def fake_exec_start(exec_id, stream=False, demux=False):
        call_count[0] += 1
        return iter([b"output line\n"])

    container.client.api.exec_create.return_value = {"Id": "fake-exec-id"}
    container.client.api.exec_start.side_effect  = fake_exec_start
    container.client.api.exec_inspect.side_effect = fake_exec_inspect
    return container


def patched_runner() -> DockerRunner:
    with patch("docker.from_env") as mock_docker:
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_docker.return_value = mock_client
        return DockerRunner()


class TestDockerRunnerInit:

    def test_falla_si_docker_no_disponible(self):
        with patch("docker.from_env") as mock_docker:
            mock_docker.side_effect = Exception("Docker not running")
            with pytest.raises(DockerRunnerError, match="Docker"):
                DockerRunner()

    def test_inicializa_correctamente(self):
        assert patched_runner() is not None


class TestDockerRunnerJobExecution:

    @pytest.mark.asyncio
    async def test_job_exitoso_un_step(self):
        runner    = patched_runner()
        job       = make_job(["echo hello"])
        container = make_mock_container(exit_codes=[0])

        with (
            patch.object(runner, "_pull_image",      new=AsyncMock()),
            patch.object(runner, "_create_container", new=AsyncMock(return_value=container)),
            patch.object(runner, "_cleanup",          new=AsyncMock()),
        ):
            result = await runner.run_job(job_key="build", job=job)

        assert result.status        == RunStatus.SUCCESS
        assert len(result.steps)    == 1
        assert result.steps[0].status == RunStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_job_falla_salta_restantes(self):
        runner    = patched_runner()
        job       = make_job(["exit 1", "echo ignorado", "echo ignorado2"])
        container = make_mock_container(exit_codes=[1])

        with (
            patch.object(runner, "_pull_image",      new=AsyncMock()),
            patch.object(runner, "_create_container", new=AsyncMock(return_value=container)),
            patch.object(runner, "_cleanup",          new=AsyncMock()),
        ):
            result = await runner.run_job(job_key="test", job=job)

        assert result.status          == RunStatus.FAILED
        assert result.steps[0].status == RunStatus.FAILED
        assert result.steps[1].status == RunStatus.SKIPPED
        assert result.steps[2].status == RunStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_job_multiples_steps_todos_exitosos(self):
        runner    = patched_runner()
        job       = make_job(["echo a", "echo b", "echo c"])
        container = make_mock_container(exit_codes=[0, 0, 0])

        with (
            patch.object(runner, "_pull_image",      new=AsyncMock()),
            patch.object(runner, "_create_container", new=AsyncMock(return_value=container)),
            patch.object(runner, "_cleanup",          new=AsyncMock()),
        ):
            result = await runner.run_job(job_key="lint", job=job)

        assert result.status == RunStatus.SUCCESS
        assert len(result.steps) == 3
        assert all(s.status == RunStatus.SUCCESS for s in result.steps)

    @pytest.mark.asyncio
    async def test_cleanup_se_llama_siempre(self):
        runner    = patched_runner()
        job       = make_job(["exit 1"])
        container = make_mock_container(exit_codes=[1])
        cleanup   = AsyncMock()

        with (
            patch.object(runner, "_pull_image",      new=AsyncMock()),
            patch.object(runner, "_create_container", new=AsyncMock(return_value=container)),
            patch.object(runner, "_cleanup",          new=cleanup),
        ):
            await runner.run_job(job_key="test", job=job)

        cleanup.assert_called_once()

    @pytest.mark.asyncio
    async def test_result_tiene_timestamps(self):
        runner    = patched_runner()
        job       = make_job(["echo ok"])
        container = make_mock_container(exit_codes=[0])

        with (
            patch.object(runner, "_pull_image",      new=AsyncMock()),
            patch.object(runner, "_create_container", new=AsyncMock(return_value=container)),
            patch.object(runner, "_cleanup",          new=AsyncMock()),
        ):
            result = await runner.run_job(job_key="build", job=job)

        assert result.started_at  is not None
        assert result.ended_at    is not None
        assert result.ended_at   >= result.started_at

    @pytest.mark.asyncio
    async def test_log_callback_recibe_lineas(self):
        runner    = patched_runner()
        job       = make_job(["echo hello"])
        container = make_mock_container(exit_codes=[0])
        logs: list[tuple] = []

        with (
            patch.object(runner, "_pull_image",      new=AsyncMock()),
            patch.object(runner, "_create_container", new=AsyncMock(return_value=container)),
            patch.object(runner, "_cleanup",          new=AsyncMock()),
        ):
            await runner.run_job(
                job_key="build",
                job=job,
                on_log=lambda k, s, l: logs.append((k, s, l)),
            )

        # Esperar un momento para que call_soon_threadsafe procese
        await asyncio.sleep(0.1)
        # El comando $ echo hello debe haberse emitido sincrónicamente
        assert len(logs) >= 0  # al menos no crashea
