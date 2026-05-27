"""
Parser de pipelines YAML.
Lee un archivo pipeline.yml y lo convierte en un PipelineDefinition validado.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from orchestrator.core.models import (
    JobDefinition,
    PipelineDefinition,
    PipelineTriggers,
    PushTrigger,
    StepDefinition,
)


class PipelineParseError(Exception):
    """Se lanza cuando el YAML tiene errores de sintaxis o de esquema."""
    pass


class PipelineParser:
    """
    Convierte un archivo pipeline.yml en un PipelineDefinition validado.

    Uso:
        parser = PipelineParser()
        pipeline = parser.parse_file("pipeline.yml")
        pipeline = parser.parse_string(yaml_text)
    """

    def parse_file(self, path: str | Path) -> PipelineDefinition:
        path = Path(path)
        if not path.exists():
            raise PipelineParseError(f"Archivo no encontrado: {path}")
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as e:
            raise PipelineParseError(f"Error de sintaxis YAML: {e}") from e
        return self._build(raw, source=str(path))

    def parse_string(self, content: str) -> PipelineDefinition:
        try:
            raw = yaml.safe_load(content)
        except yaml.YAMLError as e:
            raise PipelineParseError(f"Error de sintaxis YAML: {e}") from e
        return self._build(raw, source="<string>")

    # ------------------------------------------------------------------
    # Construcción interna
    # ------------------------------------------------------------------

    def _build(self, raw: Any, source: str) -> PipelineDefinition:
        if not isinstance(raw, dict):
            raise PipelineParseError(f"El pipeline debe ser un objeto YAML, got: {type(raw)}")

        try:
            triggers   = self._parse_triggers(raw.get("on", {}))
            global_env = self._parse_env(raw.get("env", {}))
            jobs       = self._parse_jobs(raw.get("jobs", {}), global_env)

            return PipelineDefinition(
                name=raw.get("name", "pipeline-sin-nombre"),
                description=raw.get("description", ""),
                on=triggers,
                env=global_env,
                jobs=jobs,
            )
        except ValidationError as e:
            raise PipelineParseError(
                f"Error de validación en {source}:\n{e}"
            ) from e

    def _parse_triggers(self, raw: Any) -> PipelineTriggers:
        if not raw:
            return PipelineTriggers(manual=True)

        push = None
        if "push" in raw:
            push_raw = raw["push"] or {}
            push = PushTrigger(
                branches=push_raw.get("branches", ["main"]),
                tags=push_raw.get("tags", []),
            )

        return PipelineTriggers(
            push=push,
            manual=raw.get("manual", True),
        )

    def _parse_env(self, raw: Any) -> dict[str, str]:
        if not raw:
            return {}
        if not isinstance(raw, dict):
            raise PipelineParseError("'env' debe ser un objeto clave-valor")
        return {str(k): str(v) for k, v in raw.items()}

    def _parse_jobs(
        self,
        raw: Any,
        global_env: dict[str, str],
    ) -> dict[str, JobDefinition]:
        if not raw or not isinstance(raw, dict):
            raise PipelineParseError("El pipeline debe tener al menos un job")

        jobs: dict[str, JobDefinition] = {}
        for job_key, job_raw in raw.items():
            if not isinstance(job_raw, dict):
                raise PipelineParseError(f"Job '{job_key}' debe ser un objeto")
            jobs[job_key] = self._parse_job(job_key, job_raw)

        self._validate_needs_references(jobs)
        return jobs

    def _parse_job(self, job_key: str, raw: dict) -> JobDefinition:
        steps_raw = raw.get("steps", [])
        if not steps_raw:
            raise PipelineParseError(f"Job '{job_key}' debe tener al menos un step")

        steps = [self._parse_step(job_key, i, s) for i, s in enumerate(steps_raw)]

        return JobDefinition(
            name=raw.get("name", job_key),
            image=raw.get("image", "python:3.11-slim"),
            steps=steps,
            needs=raw.get("needs", []),
            env=self._parse_env(raw.get("env", {})),
            condition=raw.get("condition"),
        )

    def _parse_step(self, job_key: str, idx: int, raw: Any) -> StepDefinition:
        if not isinstance(raw, dict):
            raise PipelineParseError(
                f"Step {idx} del job '{job_key}' debe ser un objeto"
            )
        if "run" not in raw:
            raise PipelineParseError(
                f"Step {idx} del job '{job_key}' debe tener 'run'"
            )
        return StepDefinition(
            name=raw.get("name", f"step-{idx}"),
            run=raw["run"],
            env=self._parse_env(raw.get("env", {})),
            timeout=int(raw.get("timeout", 600)),
            artifact=raw.get("artifact"),
        )

    def _validate_needs_references(self, jobs: dict[str, JobDefinition]) -> None:
        """Verifica que todos los 'needs' apunten a jobs que existen."""
        job_keys = set(jobs.keys())
        for job_key, job in jobs.items():
            for dep in job.needs:
                if dep not in job_keys:
                    raise PipelineParseError(
                        f"Job '{job_key}' depende de '{dep}', que no existe en el pipeline"
                    )
            if job_key in job.needs:
                raise PipelineParseError(
                    f"Job '{job_key}' no puede depender de sí mismo"
                )
