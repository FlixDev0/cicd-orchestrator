"""
Modelos ORM de SQLAlchemy — define las tablas de la base de datos.

Tablas:
  pipelines  — definiciones de pipelines registrados
  runs       — ejecuciones de pipelines
  job_results — resultado de cada job por run
  step_results — resultado de cada step por job
"""

from __future__ import annotations
from datetime import datetime
from sqlalchemy import (
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class PipelineORM(Base):
    __tablename__ = "pipelines"

    id:          Mapped[str]      = mapped_column(String(36), primary_key=True)
    name:        Mapped[str]      = mapped_column(String(255), nullable=False)
    description: Mapped[str]      = mapped_column(Text, default="")
    yaml_content: Mapped[str]     = mapped_column(Text, nullable=False)
    created_at:  Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    runs: Mapped[list[RunORM]] = relationship("RunORM", back_populates="pipeline")


class RunORM(Base):
    __tablename__ = "runs"

    id:          Mapped[str]           = mapped_column(String(36), primary_key=True)
    pipeline_id: Mapped[str]           = mapped_column(
        String(36), ForeignKey("pipelines.id"), nullable=False
    )
    status:      Mapped[str]           = mapped_column(String(20), default="pending")
    trigger:     Mapped[str]           = mapped_column(String(20), default="manual")
    branch:      Mapped[str]           = mapped_column(String(255), default="main")
    commit_sha:  Mapped[str | None]    = mapped_column(String(40), nullable=True)
    started_at:  Mapped[datetime]      = mapped_column(DateTime, default=datetime.utcnow)
    ended_at:    Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    pipeline: Mapped[PipelineORM]         = relationship("PipelineORM", back_populates="runs")
    jobs:     Mapped[list[JobResultORM]]  = relationship("JobResultORM", back_populates="run")


class JobResultORM(Base):
    __tablename__ = "job_results"

    id:         Mapped[str]           = mapped_column(String(36), primary_key=True)
    run_id:     Mapped[str]           = mapped_column(
        String(36), ForeignKey("runs.id"), nullable=False
    )
    job_key:    Mapped[str]           = mapped_column(String(255), nullable=False)
    status:     Mapped[str]           = mapped_column(String(20), default="pending")
    started_at: Mapped[datetime]      = mapped_column(DateTime, default=datetime.utcnow)
    ended_at:   Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    run:   Mapped[RunORM]               = relationship("RunORM", back_populates="jobs")
    steps: Mapped[list[StepResultORM]]  = relationship("StepResultORM", back_populates="job")


class StepResultORM(Base):
    __tablename__ = "step_results"

    id:         Mapped[str]           = mapped_column(String(36), primary_key=True)
    job_id:     Mapped[str]           = mapped_column(
        String(36), ForeignKey("job_results.id"), nullable=False
    )
    step_name:  Mapped[str]           = mapped_column(String(255), nullable=False)
    status:     Mapped[str]           = mapped_column(String(20), default="pending")
    exit_code:  Mapped[int]           = mapped_column(Integer, default=-1)
    started_at: Mapped[datetime]      = mapped_column(DateTime, default=datetime.utcnow)
    ended_at:   Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    job: Mapped[JobResultORM] = relationship("JobResultORM", back_populates="steps")
