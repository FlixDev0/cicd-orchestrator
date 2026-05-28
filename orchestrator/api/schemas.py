"""
Schemas de la API — modelos Pydantic para requests y responses.

Separados de los modelos de dominio (core/models.py) para que
la API pueda evolucionar independientemente del dominio interno.
"""

from __future__ import annotations
from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------

class CreatePipelineRequest(BaseModel):
    """Body del POST /pipelines — envía el YAML del pipeline."""
    yaml_content: str = Field(
        ...,
        description="Contenido YAML del pipeline",
        min_length=10,
    )


class TriggerRunRequest(BaseModel):
    """Body del POST /pipelines/{id}/runs — dispara una ejecución."""
    branch:     str = Field(default="main", description="Rama del repositorio")
    trigger:    str = Field(default="manual", description="Tipo de trigger")
    commit_sha: str | None = Field(default=None, description="SHA del commit")


# ---------------------------------------------------------------------------
# Responses
# ---------------------------------------------------------------------------

class PipelineResponse(BaseModel):
    """Respuesta al crear o consultar un pipeline."""
    id:          str
    name:        str
    description: str
    created_at:  str

    class Config:
        from_attributes = True


class StepResultResponse(BaseModel):
    step_name:  str
    status:     str
    exit_code:  int
    started_at: datetime | None = None
    ended_at:   datetime | None = None


class JobResultResponse(BaseModel):
    job_key:    str
    status:     str
    started_at: datetime | None = None
    ended_at:   datetime | None = None
    duration:   float | None    = None
    steps:      list[StepResultResponse] = []


class RunResponse(BaseModel):
    """Respuesta al crear o consultar un run."""
    id:          str
    pipeline_id: str
    status:      str
    trigger:     str
    branch:      str
    started_at:  datetime
    ended_at:    datetime | None = None
    duration:    float | None    = None
    jobs:        dict[str, JobResultResponse] = {}


class ErrorResponse(BaseModel):
    detail: str


# ---------------------------------------------------------------------------
# Helpers de conversión dominio → response
# ---------------------------------------------------------------------------

def run_to_response(run) -> RunResponse:
    """Convierte un PipelineRun del dominio a RunResponse de la API."""
    duration = None
    if run.ended_at:
        duration = (run.ended_at - run.started_at).total_seconds()

    jobs_resp = {}
    for key, job in run.jobs.items():
        steps_resp = [
            StepResultResponse(
                step_name=s.step_name,
                status=s.status.value,
                exit_code=s.exit_code,
                started_at=s.started_at,
                ended_at=s.ended_at,
            )
            for s in job.steps
        ]
        jobs_resp[key] = JobResultResponse(
            job_key=key,
            status=job.status.value,
            started_at=job.started_at,
            ended_at=job.ended_at,
            duration=job.duration_seconds,
            steps=steps_resp,
        )

    return RunResponse(
        id=str(run.id),
        pipeline_id=str(run.pipeline_id),
        status=run.status.value,
        trigger=run.trigger.value,
        branch=run.branch,
        started_at=run.started_at,
        ended_at=run.ended_at,
        duration=duration,
        jobs=jobs_resp,
    )
