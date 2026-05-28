"""
Store en memoria — gestiona el estado de pipelines y runs.

Por ahora usamos un store en memoria simple (diccionarios).
En la siguiente fase esto se reemplaza por SQLAlchemy + PostgreSQL,
pero la interfaz del store no cambia — el resto de la API no nota la diferencia.
"""

from __future__ import annotations
import uuid
from datetime import datetime
from typing import Any
from orchestrator.core.models import PipelineDefinition, PipelineRun, RunStatus


class NotFoundError(Exception):
    pass


class Store:
    """
    Store en memoria para pipelines y runs.
    Thread-safe para operaciones simples (asyncio es single-threaded).
    """

    def __init__(self) -> None:
        # pipeline_id (str) → {"definition": PipelineDefinition, "name": str, ...}
        self._pipelines: dict[str, dict[str, Any]] = {}
        # run_id (str) → PipelineRun
        self._runs: dict[str, PipelineRun] = {}

    # ------------------------------------------------------------------
    # Pipelines
    # ------------------------------------------------------------------

    def save_pipeline(
        self,
        pipeline_id: str,
        definition:  PipelineDefinition,
        yaml_content: str,
    ) -> dict[str, Any]:
        record = {
            "id":           pipeline_id,
            "name":         definition.name,
            "description":  definition.description,
            "yaml":         yaml_content,
            "definition":   definition,
            "created_at":   datetime.utcnow().isoformat(),
        }
        self._pipelines[pipeline_id] = record
        return record

    def get_pipeline(self, pipeline_id: str) -> dict[str, Any]:
        if pipeline_id not in self._pipelines:
            raise NotFoundError(f"Pipeline '{pipeline_id}' no encontrado")
        return self._pipelines[pipeline_id]

    def list_pipelines(self) -> list[dict[str, Any]]:
        return [
            {k: v for k, v in p.items() if k != "definition"}
            for p in self._pipelines.values()
        ]

    # ------------------------------------------------------------------
    # Runs
    # ------------------------------------------------------------------

    def save_run(self, run: PipelineRun) -> PipelineRun:
        self._runs[str(run.id)] = run
        return run

    def get_run(self, run_id: str) -> PipelineRun:
        if run_id not in self._runs:
            raise NotFoundError(f"Run '{run_id}' no encontrado")
        return self._runs[run_id]

    def list_runs(self, pipeline_id: str | None = None) -> list[PipelineRun]:
        runs = list(self._runs.values())
        if pipeline_id:
            runs = [r for r in runs if str(r.pipeline_id) == pipeline_id]
        return sorted(runs, key=lambda r: r.started_at, reverse=True)


# Instancia global del store (singleton para toda la app)
store = Store()
