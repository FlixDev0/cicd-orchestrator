"""
Store con persistencia en base de datos.

Implementa la misma interfaz que el store en memoria (api/store.py),
por lo que app.py no necesita cambiar nada — solo importa desde aquí.

Convierte entre:
  - Modelos de dominio (PipelineDefinition, PipelineRun) ← lo que usa el sistema
  - Modelos ORM (PipelineORM, RunORM)                    ← lo que guarda la DB
"""

from __future__ import annotations
import uuid
from datetime import datetime
from typing import Any
from orchestrator.core.models import (
    JobResult,
    PipelineDefinition,
    PipelineRun,
    RunStatus,
    StepResult,
    TriggerType,
)
from orchestrator.core.parser import PipelineParser
from orchestrator.storage.database import get_session
from orchestrator.storage.orm_models import (
    JobResultORM,
    PipelineORM,
    RunORM,
    StepResultORM,
)


class NotFoundError(Exception):
    pass


class DatabaseStore:
    """
    Store persistente con SQLAlchemy.
    Misma interfaz que el Store en memoria de api/store.py.
    """

    def __init__(self) -> None:
        self._parser = PipelineParser()

    # ------------------------------------------------------------------
    # Pipelines
    # ------------------------------------------------------------------

    def save_pipeline(
        self,
        pipeline_id:  str,
        definition:   PipelineDefinition,
        yaml_content: str,
    ) -> dict[str, Any]:
        with get_session() as session:
            # Upsert — actualiza si ya existe
            existing = session.get(PipelineORM, pipeline_id)
            if existing:
                existing.name         = definition.name
                existing.description  = definition.description
                existing.yaml_content = yaml_content
                record = existing
            else:
                record = PipelineORM(
                    id=pipeline_id,
                    name=definition.name,
                    description=definition.description,
                    yaml_content=yaml_content,
                )
                session.add(record)

            session.flush()
            return self._pipeline_to_dict(record, definition)

    def get_pipeline(self, pipeline_id: str) -> dict[str, Any]:
        with get_session() as session:
            record = session.get(PipelineORM, pipeline_id)
            if not record:
                raise NotFoundError(f"Pipeline '{pipeline_id}' no encontrado")

            # Re-parsear el YAML para obtener el PipelineDefinition
            definition = self._parser.parse_string(record.yaml_content)
            return self._pipeline_to_dict(record, definition)

    def list_pipelines(self) -> list[dict[str, Any]]:
        with get_session() as session:
            records = session.query(PipelineORM).order_by(
                PipelineORM.created_at.desc()
            ).all()
            result = []
            for r in records:
                result.append({
                    "id":          r.id,
                    "name":        r.name,
                    "description": r.description,
                    "yaml":        r.yaml_content,
                    "created_at":  r.created_at.isoformat(),
                })
            return result

    # ------------------------------------------------------------------
    # Runs
    # ------------------------------------------------------------------

    def save_run(self, run: PipelineRun) -> PipelineRun:
        with get_session() as session:
            run_id = str(run.id)
            existing = session.get(RunORM, run_id)

            if existing:
                existing.status   = run.status.value
                existing.ended_at = run.ended_at
            else:
                orm_run = RunORM(
                    id=run_id,
                    pipeline_id=str(run.pipeline_id),
                    status=run.status.value,
                    trigger=run.trigger.value,
                    branch=run.branch,
                    commit_sha=run.commit_sha,
                    started_at=run.started_at,
                    ended_at=run.ended_at,
                )
                session.add(orm_run)
                session.flush()

            # Persistir jobs siempre (tanto en create como en update)
            for job_key, job_result in run.jobs.items():
                self._upsert_job_result(session, run_id, job_key, job_result)

        return run

    def get_run(self, run_id: str) -> PipelineRun:
        with get_session() as session:
            orm_run = session.get(RunORM, run_id)
            if not orm_run:
                raise NotFoundError(f"Run '{run_id}' no encontrado")

            # Cargar jobs y steps
            job_results = {}
            for orm_job in orm_run.jobs:
                steps = [
                    StepResult(
                        step_name=s.step_name,
                        status=RunStatus(s.status),
                        exit_code=s.exit_code,
                        started_at=s.started_at,
                        ended_at=s.ended_at,
                    )
                    for s in orm_job.steps
                ]
                job_results[orm_job.job_key] = JobResult(
                    job_key=orm_job.job_key,
                    status=RunStatus(orm_job.status),
                    started_at=orm_job.started_at,
                    ended_at=orm_job.ended_at,
                    steps=steps,
                )

            return PipelineRun(
                id=uuid.UUID(orm_run.id),
                pipeline_id=uuid.UUID(orm_run.pipeline_id),
                status=RunStatus(orm_run.status),
                trigger=TriggerType(orm_run.trigger),
                branch=orm_run.branch,
                commit_sha=orm_run.commit_sha,
                started_at=orm_run.started_at,
                ended_at=orm_run.ended_at,
                jobs=job_results,
            )

    def list_runs(self, pipeline_id: str | None = None) -> list[PipelineRun]:
        with get_session() as session:
            query = session.query(RunORM).order_by(RunORM.started_at.desc())
            if pipeline_id:
                query = query.filter(RunORM.pipeline_id == pipeline_id)

            run_ids = [r.id for r in query.all()]

        # Cargar cada run completo (con jobs y steps)
        return [self.get_run(run_id) for run_id in run_ids]

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    def _upsert_job_result(
        self,
        session,
        run_id:     str,
        job_key:    str,
        job_result: JobResult,
    ) -> None:
        """Inserta o actualiza un JobResult en la DB."""
        # Buscar si ya existe el job para este run
        existing = session.query(JobResultORM).filter_by(
            run_id=run_id, job_key=job_key
        ).first()

        if existing:
            existing.status   = job_result.status.value
            existing.ended_at = job_result.ended_at
            job_orm = existing
        else:
            job_orm = JobResultORM(
                id=str(uuid.uuid4()),
                run_id=run_id,
                job_key=job_key,
                status=job_result.status.value,
                started_at=job_result.started_at,
                ended_at=job_result.ended_at,
            )
            session.add(job_orm)
            session.flush()

        # Insertar steps (solo si no existen aún)
        existing_steps = {s.step_name for s in job_orm.steps}
        for step in job_result.steps:
            if step.step_name not in existing_steps:
                session.add(StepResultORM(
                    id=str(uuid.uuid4()),
                    job_id=job_orm.id,
                    step_name=step.step_name,
                    status=step.status.value,
                    exit_code=step.exit_code,
                    started_at=step.started_at,
                    ended_at=step.ended_at,
                ))

    @staticmethod
    def _pipeline_to_dict(
        record:     PipelineORM,
        definition: PipelineDefinition,
    ) -> dict[str, Any]:
        return {
            "id":          record.id,
            "name":        record.name,
            "description": record.description,
            "yaml":        record.yaml_content,
            "definition":  definition,
            "created_at":  record.created_at.isoformat()
                           if isinstance(record.created_at, datetime)
                           else record.created_at,
        }


# Instancia global
store = DatabaseStore()
