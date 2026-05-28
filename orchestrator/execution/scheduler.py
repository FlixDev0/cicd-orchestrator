"""
Scheduler — coordina la ejecución paralela de jobs usando el DAG engine.

Estrategia: ejecución por "olas" (waves).
  - Cada ola contiene los jobs que pueden correr en paralelo.
  - Todos los jobs de una ola se lanzan simultáneamente con asyncio.gather.
  - Cuando toda la ola termina, se evalúa la siguiente.
  - Si algún job falló, sus dependientes en olas futuras se marcan SKIPPED.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime
from typing import Any

from orchestrator.core.dag import DAGEngine
from orchestrator.core.models import (
    JobResult,
    PipelineDefinition,
    PipelineRun,
    RunStatus,
    TriggerType,
)
from orchestrator.execution.docker_runner import DockerRunner


EventCallback = Callable[[str, dict[str, Any]], None]


class Scheduler:
    """
    Orquesta la ejecución completa de un pipeline por olas paralelas.

    Uso:
        scheduler = Scheduler(runner=DockerRunner())
        run = await scheduler.execute(pipeline, on_event=handler)
    """

    def __init__(self, runner: DockerRunner) -> None:
        self._runner = runner

    async def execute(
        self,
        pipeline:  PipelineDefinition,
        trigger:   TriggerType       = TriggerType.MANUAL,
        branch:    str               = "main",
        on_event:  EventCallback | None = None,
    ) -> PipelineRun:
        import uuid

        dag   = DAGEngine(pipeline)
        waves = dag.execution_waves()

        run = PipelineRun(
            pipeline_id=uuid.uuid4(),
            trigger=trigger,
            branch=branch,
            status=RunStatus.RUNNING,
            started_at=datetime.utcnow(),
        )

        self._emit(on_event, "run_started", {
            "run_id":     str(run.id),
            "pipeline":   pipeline.name,
            "total_jobs": len(pipeline.jobs),
            "waves":      waves,
        })

        failed:  set[str] = set()
        skipped: set[str] = set()

        for wave in waves:
            # Separar jobs que se ejecutan vs los que se saltean por fallo upstream
            to_run:  list[str] = []
            to_skip: list[str] = []

            for job_key in wave:
                deps = dag.dependencies_of(job_key)
                if any(d in failed or d in skipped for d in deps):
                    to_skip.append(job_key)
                else:
                    to_run.append(job_key)

            # Marcar los saltados inmediatamente
            for job_key in to_skip:
                skipped.add(job_key)
                run.jobs[job_key] = JobResult(
                    job_key=job_key,
                    status=RunStatus.SKIPPED,
                    ended_at=datetime.utcnow(),
                )
                self._emit(on_event, "job_finished", {
                    "run_id":   str(run.id),
                    "job_key":  job_key,
                    "status":   RunStatus.SKIPPED.value,
                    "duration": 0,
                })

            # Lanzar los jobs listos en paralelo
            if to_run:
                results = await asyncio.gather(
                    *[self._run_one(run, pipeline, job_key, on_event)
                      for job_key in to_run],
                    return_exceptions=False,
                )
                for result in results:
                    run.jobs[result.job_key] = result
                    if result.status == RunStatus.FAILED:
                        failed.add(result.job_key)

        # Estado final
        run.ended_at = datetime.utcnow()
        run.status   = RunStatus.FAILED if failed else RunStatus.SUCCESS

        self._emit(on_event, "run_finished", {
            "run_id":   str(run.id),
            "status":   run.status.value,
            "duration": (run.ended_at - run.started_at).total_seconds(),
            "jobs":     {k: v.status.value for k, v in run.jobs.items()},
        })

        return run

    async def _run_one(
        self,
        run:      PipelineRun,
        pipeline: PipelineDefinition,
        job_key:  str,
        on_event: EventCallback | None,
    ) -> JobResult:
        """Ejecuta un único job y emite eventos de progreso."""
        job = pipeline.jobs[job_key]
        env = pipeline.merge_env(job_key)

        self._emit(on_event, "job_started", {
            "run_id":  str(run.id),
            "job_key": job_key,
            "image":   job.image,
        })

        def on_log(key: str, step: str, line: str) -> None:
            self._emit(on_event, "log", {
                "run_id":  str(run.id),
                "job_key": key,
                "step":    step,
                "line":    line,
            })

        try:
            result = await self._runner.run_job(
                job_key=job_key,
                job=job,
                env=env,
                on_log=on_log,
            )
        except Exception:
            result = JobResult(
                job_key=job_key,
                status=RunStatus.FAILED,
                ended_at=datetime.utcnow(),
            )

        self._emit(on_event, "job_finished", {
            "run_id":   str(run.id),
            "job_key":  job_key,
            "status":   result.status.value,
            "duration": result.duration_seconds,
        })

        return result

    @staticmethod
    def _emit(
        on_event: EventCallback | None,
        event:    str,
        data:     dict[str, Any],
    ) -> None:
        if on_event:
            on_event(event, data)
