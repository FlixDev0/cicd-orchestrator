"""
Docker Runner — ejecuta un JobDefinition dentro de un contenedor Docker real.

Responsabilidades:
  1. Pull de la imagen si no está en caché local
  2. Crear el contenedor con env y volúmenes
  3. Ejecutar cada step en secuencia, capturando logs en tiempo real
  4. Verificar exit codes y respetar timeouts
  5. Destruir el contenedor al finalizar (siempre, incluso si falla)
  6. Retornar un JobResult con el estado final
"""

from __future__ import annotations

import asyncio
import threading
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


# Tipo para el callback de logs: recibe (job_key, step_name, línea de log)
LogCallback = Callable[[str, str, str], None]


class DockerRunnerError(Exception):
    """Error del Docker Runner."""
    pass


class DockerRunner:
    """
    Ejecuta un job dentro de un contenedor Docker.

    Uso:
        runner = DockerRunner()
        result = await runner.run_job(
            job_key="build",
            job=pipeline.jobs["build"],
            env=pipeline.merge_env("build"),
            on_log=lambda key, step, line: print(f"[{key}/{step}] {line}"),
        )
    """

    def __init__(self, workspace_dir: str = "/tmp/cicd-workspace") -> None:
        self._workspace = workspace_dir
        try:
            self._client = docker.from_env()
            self._client.ping()
        except Exception as e:
            raise DockerRunnerError(
                f"No se pudo conectar a Docker. ¿Está Docker Desktop corriendo?\n{e}"
            ) from e

    # ------------------------------------------------------------------
    # Punto de entrada principal
    # ------------------------------------------------------------------

    async def run_job(
        self,
        job_key:   str,
        job:       JobDefinition,
        env:       dict[str, str] | None = None,
        on_log:    LogCallback | None    = None,
    ) -> JobResult:
        """
        Ejecuta todos los steps de un job en un contenedor Docker.
        Retorna un JobResult con el estado final y los resultados por step.
        """
        result = JobResult(
            job_key=job_key,
            status=RunStatus.RUNNING,
            started_at=datetime.utcnow(),
        )
        container: Container | None = None

        try:
            # 1. Pull de la imagen (no bloquea si ya está en caché)
            await self._pull_image(job.image, job_key, on_log)

            # 2. Crear el contenedor (sin iniciarlo aún)
            container = await self._create_container(job, env or {}, job_key)

            # 3. Ejecutar los steps uno a uno
            for step in job.steps:
                step_result = await self._run_step(
                    container=container,
                    job_key=job_key,
                    step_name=step.name,
                    command=step.run,
                    timeout=step.timeout,
                    on_log=on_log,
                )
                result.steps.append(step_result)

                # Si un step falla, los siguientes se marcan como skipped
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
            # 4. Destruir el contenedor siempre, pase lo que pase
            if container:
                await self._cleanup(container, job_key, on_log)

        return result

    # ------------------------------------------------------------------
    # Pasos internos
    # ------------------------------------------------------------------

    async def _pull_image(
        self,
        image: str,
        job_key: str,
        on_log: LogCallback | None,
    ) -> None:
        """Pull de la imagen Docker en un thread para no bloquear el event loop."""
        self._emit(on_log, job_key, "runner", f"Pulling image: {image}")
        loop = asyncio.get_event_loop()

        def _do_pull():
            try:
                self._client.images.get(image)
                # Ya está en caché local, no hace pull
            except docker.errors.ImageNotFound:
                self._client.images.pull(image)

        await loop.run_in_executor(None, _do_pull)
        self._emit(on_log, job_key, "runner", f"Image ready: {image}")

    async def _create_container(
        self,
        job: JobDefinition,
        env:     dict[str, str],
        job_key: str,
    ) -> Container:
        """Crea el contenedor Docker con la configuración del job."""
        loop = asyncio.get_event_loop()

        # Nombre único para el contenedor
        container_name = f"cicd-{job_key}-{uuid.uuid4().hex[:8]}"

        # Combinar env del job con el env del run
        full_env = {**env, **job.env}

        def _create():
            return self._client.containers.create(
                image=job.image,
                name=container_name,
                environment=full_env,
                working_dir="/workspace",
                # El contenedor se mantiene vivo esperando comandos
                command="tail -f /dev/null",
                detach=True,
            )

        container = await loop.run_in_executor(None, _create)

        # Iniciar el contenedor
        await loop.run_in_executor(None, container.start)
        self._emit(on_log=None, job_key=job_key, step="runner",
                   line=f"Container started: {container_name}")
        return container

    async def _run_step(
        self,
        container: Container,
        job_key:   str,
        step_name: str,
        command:   str,
        timeout:   int,
        on_log:    LogCallback | None,
    ) -> StepResult:
        """Ejecuta un comando dentro del contenedor y transmite los logs."""
        loop = asyncio.get_event_loop()
        result = StepResult(
            step_name=step_name,
            status=RunStatus.RUNNING,
            started_at=datetime.utcnow(),
        )

        self._emit(on_log, job_key, step_name, f"$ {command}")

        def _exec_and_stream():
            """
            Ejecuta el comando con exec_run y captura logs línea a línea.
            Corre en un thread para no bloquear asyncio.
            """
            exec_id = container.client.api.exec_create(
                container.id,
                cmd=["sh", "-c", command],
                stdout=True,
                stderr=True,
            )
            output = container.client.api.exec_start(
                exec_id["Id"],
                stream=True,
                demux=False,
            )
            lines = []
            for chunk in output:
                if isinstance(chunk, bytes):
                    line = chunk.decode("utf-8", errors="replace").rstrip()
                    if line:
                        lines.append(line)
                        self._emit(on_log, job_key, step_name, line)

            inspect = container.client.api.exec_inspect(exec_id["Id"])
            exit_code = inspect.get("ExitCode", -1)
            return exit_code, lines

        try:
            exit_code, _ = await asyncio.wait_for(
                loop.run_in_executor(None, _exec_and_stream),
                timeout=timeout,
            )
            result.exit_code = exit_code
            result.status = RunStatus.SUCCESS if exit_code == 0 else RunStatus.FAILED

        except asyncio.TimeoutError:
            result.exit_code = -1
            result.status = RunStatus.FAILED
            self._emit(on_log, job_key, step_name,
                       f"TIMEOUT: step superó {timeout}s")

        result.ended_at = datetime.utcnow()
        status_str = "✓" if result.status == RunStatus.SUCCESS else "✗"
        self._emit(on_log, job_key, step_name,
                   f"{status_str} exit_code={result.exit_code}")
        return result

    async def _cleanup(
        self,
        container: Container,
        job_key: str,
        on_log: LogCallback | None,
    ) -> None:
        """Detiene y elimina el contenedor."""
        loop = asyncio.get_event_loop()
        try:
            def _remove():
                container.stop(timeout=5)
                container.remove(force=True)
            await loop.run_in_executor(None, _remove)
            self._emit(on_log, job_key, "runner", "Container removed")
        except Exception:
            pass  # El cleanup nunca debe romper el flujo principal

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _emit(
        on_log:   LogCallback | None,
        job_key:  str,
        step:     str,
        line:     str,
    ) -> None:
        """Llama al callback de log si está definido."""
        if on_log:
            on_log(job_key, step, line)
