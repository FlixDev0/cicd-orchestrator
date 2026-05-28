"""
Docker Runner — ejecuta un JobDefinition dentro de un contenedor Docker real.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from datetime import datetime

import docker
import docker.errors
from docker.models.containers import Container

from orchestrator.core.models import (
    JobDefinition,
    JobResult,
    RunStatus,
    StepResult,
)

LogCallback = Callable[[str, str, str], None]


class DockerRunnerError(Exception):
    pass


class DockerRunner:

    def __init__(self, workspace_dir: str = "/tmp/cicd-workspace") -> None:
        self._workspace = workspace_dir
        try:
            self._client = docker.from_env()
            self._client.ping()
        except Exception as e:
            raise DockerRunnerError(
                f"No se pudo conectar a Docker. ¿Está Docker Desktop corriendo?\n{e}"
            ) from e

    async def run_job(
        self,
        job_key: str,
        job:     JobDefinition,
        env:     dict[str, str] | None = None,
        on_log:  LogCallback | None    = None,
    ) -> JobResult:
        result = JobResult(
            job_key=job_key,
            status=RunStatus.RUNNING,
            started_at=datetime.utcnow(),
        )
        container: Container | None = None

        # Capturamos el loop AQUÍ, en el contexto async correcto
        loop = asyncio.get_event_loop()

        # Wrapper thread-safe para emitir logs desde threads
        def emit_safe(key: str, step: str, line: str) -> None:
            if on_log:
                loop.call_soon_threadsafe(on_log, key, step, line)

        try:
            await self._pull_image(job.image, job_key, emit_safe, loop)
            container = await self._create_container(job, env or {}, job_key, loop)

            for step in job.steps:
                step_result = await self._run_step(
                    container=container,
                    job_key=job_key,
                    step_name=step.name,
                    command=step.run,
                    timeout=step.timeout,
                    emit_safe=emit_safe,
                    loop=loop,
                )
                result.steps.append(step_result)

                if step_result.status == RunStatus.FAILED:
                    for remaining in job.steps[job.steps.index(step) + 1:]:
                        result.steps.append(StepResult(
                            step_name=remaining.name,
                            status=RunStatus.SKIPPED,
                            exit_code=-1,
                        ))
                    result.status = RunStatus.FAILED
                    result.ended_at = datetime.utcnow()
                    return result

            result.status = RunStatus.SUCCESS

        except DockerRunnerError:
            result.status = RunStatus.FAILED
            raise
        except Exception as e:
            result.status = RunStatus.FAILED
            raise DockerRunnerError(f"Error inesperado en job '{job_key}': {e}") from e
        finally:
            result.ended_at = datetime.utcnow()
            if container:
                await self._cleanup(container, job_key, loop)

        return result

    async def _pull_image(
        self,
        image:     str,
        job_key:   str,
        emit_safe: Callable,
        loop:      asyncio.AbstractEventLoop,
    ) -> None:
        emit_safe(job_key, "runner", f"Pulling image: {image}")

        def _do_pull():
            try:
                self._client.images.get(image)
            except docker.errors.ImageNotFound:
                self._client.images.pull(image)

        await loop.run_in_executor(None, _do_pull)
        emit_safe(job_key, "runner", f"Image ready: {image}")

    async def _create_container(
        self,
        job:     JobDefinition,
        env:     dict[str, str],
        job_key: str,
        loop:    asyncio.AbstractEventLoop,
    ) -> Container:
        container_name = f"cicd-{job_key}-{uuid.uuid4().hex[:8]}"
        full_env = {**env, **job.env}

        def _create():
            c = self._client.containers.run(
                image=job.image,
                name=container_name,
                environment=full_env,
                working_dir="/workspace",
                command="sleep 3600",
                detach=True,
            )
            return c

        container = await loop.run_in_executor(None, _create)
        return container

    async def _run_step(
        self,
        container: Container,
        job_key:   str,
        step_name: str,
        command:   str,
        timeout:   int,
        emit_safe: Callable,
        loop:      asyncio.AbstractEventLoop,
    ) -> StepResult:
        result = StepResult(
            step_name=step_name,
            status=RunStatus.RUNNING,
            started_at=datetime.utcnow(),
        )

        emit_safe(job_key, step_name, f"$ {command}")

        def _exec_and_stream():
            exec_id = container.client.api.exec_create(
                container.id,
                cmd=["sh", "-c", command],
                stdout=True,
                stderr=True,
                tty=False,
            )
            output = container.client.api.exec_start(
                exec_id["Id"],
                stream=True,
                demux=False,
            )
            for chunk in output:
                if isinstance(chunk, bytes):
                    line = chunk.decode("utf-8", errors="replace").rstrip()
                    if line:
                        emit_safe(job_key, step_name, line)

            # Esperar hasta que el exit code esté disponible
            for _ in range(20):
                inspect = container.client.api.exec_inspect(exec_id["Id"])
                if not inspect.get("Running", True):
                    return inspect.get("ExitCode", -1)
                time.sleep(0.1)

            return -1

        try:
            exit_code = await asyncio.wait_for(
                loop.run_in_executor(None, _exec_and_stream),
                timeout=timeout,
            )
            result.exit_code = exit_code if exit_code is not None else -1
            result.status = RunStatus.SUCCESS if result.exit_code == 0 else RunStatus.FAILED

        except asyncio.TimeoutError:
            result.exit_code = -1
            result.status = RunStatus.FAILED
            emit_safe(job_key, step_name, f"TIMEOUT: step superó {timeout}s")

        result.ended_at = datetime.utcnow()
        icon = "✓" if result.status == RunStatus.SUCCESS else "✗"
        emit_safe(job_key, step_name, f"{icon} exit_code={result.exit_code}")
        return result

    async def _cleanup(
        self,
        container: Container,
        job_key:   str,
        loop:      asyncio.AbstractEventLoop,
    ) -> None:
        try:
            def _remove():
                container.stop(timeout=5)
                container.remove(force=True)
            await loop.run_in_executor(None, _remove)
        except Exception:
            pass

    @staticmethod
    def _emit(
        on_log:  LogCallback | None,
        job_key: str,
        step:    str,
        line:    str,
    ) -> None:
        if on_log:
            on_log(job_key, step, line)
