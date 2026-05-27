"""
Domain models — fuente de verdad para todas las estructuras de datos.
Estas clases Pydantic representan cómo el sistema entiende un pipeline
después de parsear el YAML. Son independientes de la base de datos.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Enumeraciones de estado
# ---------------------------------------------------------------------------

class RunStatus(str, Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    SUCCESS   = "success"
    FAILED    = "failed"
    CANCELLED = "cancelled"
    SKIPPED   = "skipped"


class TriggerType(str, Enum):
    PUSH    = "push"
    MANUAL  = "manual"
    WEBHOOK = "webhook"
    API     = "api"


# ---------------------------------------------------------------------------
# Definición de un Step (unidad mínima de ejecución)
# ---------------------------------------------------------------------------

class StepDefinition(BaseModel):
    """Un step es un comando que se ejecuta dentro de un job."""
    name:    str
    run:     str                              # comando shell a ejecutar
    env:     dict[str, str]   = Field(default_factory=dict)
    timeout: int              = 600           # segundos (default 10 min)
    artifact: str | None      = None          # archivo a marcar como artefacto


# ---------------------------------------------------------------------------
# Definición de un Job
# ---------------------------------------------------------------------------

class JobDefinition(BaseModel):
    """
    Un job es un grupo de steps que corren en el mismo contenedor Docker.
    Los jobs pueden correr en paralelo si no tienen dependencias entre sí.
    """
    name:      str
    image:     str                                     # ej: "python:3.11-slim"
    steps:     list[StepDefinition]
    needs:     list[str]        = Field(default_factory=list)   # nombres de jobs dependientes
    env:       dict[str, str]   = Field(default_factory=dict)
    condition: str | None       = None                  # expresión condicional, ej: "branch == 'main'"

    @field_validator("steps")
    @classmethod
    def steps_not_empty(cls, v: list) -> list:
        if not v:
            raise ValueError("Un job debe tener al menos un step")
        return v


# ---------------------------------------------------------------------------
# Triggers — cuándo se activa el pipeline
# ---------------------------------------------------------------------------

class PushTrigger(BaseModel):
    branches: list[str] = Field(default_factory=lambda: ["main"])
    tags:     list[str] = Field(default_factory=list)


class PipelineTriggers(BaseModel):
    push:   PushTrigger | None = None
    manual: bool               = True     # siempre se puede disparar manualmente


# ---------------------------------------------------------------------------
# Definición completa de un Pipeline (resultado del parse del YAML)
# ---------------------------------------------------------------------------

class PipelineDefinition(BaseModel):
    """
    Representa la definición completa de un pipeline leída desde pipeline.yml.
    Esta es la estructura que el Parser produce y el DAG engine consume.
    """
    name:        str
    description: str                       = ""
    on:          PipelineTriggers          = Field(default_factory=PipelineTriggers)
    env:         dict[str, str]            = Field(default_factory=dict)   # vars globales
    jobs:        dict[str, JobDefinition]  = Field(default_factory=dict)

    @field_validator("jobs")
    @classmethod
    def jobs_not_empty(cls, v: dict) -> dict:
        if not v:
            raise ValueError("El pipeline debe tener al menos un job")
        return v

    def job_names(self) -> list[str]:
        return list(self.jobs.keys())

    def merge_env(self, job_key: str) -> dict[str, str]:
        """Retorna el env efectivo para un job: global sobreescrito por local."""
        job = self.jobs[job_key]
        return {**self.env, **job.env}


# ---------------------------------------------------------------------------
# Modelos de estado en tiempo de ejecución (runtime state)
# ---------------------------------------------------------------------------

class StepResult(BaseModel):
    """Estado y resultado de un step después de ejecutarse."""
    step_name:  str
    status:     RunStatus
    exit_code:  int          = -1
    started_at: datetime     = Field(default_factory=datetime.utcnow)
    ended_at:   datetime | None = None
    artifact:   str | None   = None


class JobResult(BaseModel):
    """Estado y resultado de un job después de ejecutarse."""
    job_key:    str
    status:     RunStatus
    started_at: datetime     = Field(default_factory=datetime.utcnow)
    ended_at:   datetime | None = None
    steps:      list[StepResult] = Field(default_factory=list)

    @property
    def duration_seconds(self) -> float | None:
        if self.ended_at:
            return (self.ended_at - self.started_at).total_seconds()
        return None


class PipelineRun(BaseModel):
    """Representa una ejecución concreta de un pipeline."""
    id:          uuid.UUID       = Field(default_factory=uuid.uuid4)
    pipeline_id: uuid.UUID
    status:      RunStatus       = RunStatus.PENDING
    trigger:     TriggerType     = TriggerType.MANUAL
    branch:      str             = "main"
    commit_sha:  str | None      = None
    started_at:  datetime        = Field(default_factory=datetime.utcnow)
    ended_at:    datetime | None = None
    jobs:        dict[str, JobResult] = Field(default_factory=dict)

    @property
    def is_terminal(self) -> bool:
        """True si el run ya terminó (éxito, fallo o cancelado)."""
        return self.status in {
            RunStatus.SUCCESS,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
        }
