"""
API REST del orquestador CI/CD.

Endpoints:
  POST   /pipelines                    — registrar un pipeline (YAML)
  GET    /pipelines                    — listar pipelines
  GET    /pipelines/{id}               — detalle de un pipeline
  POST   /pipelines/{id}/runs          — disparar una ejecución
  GET    /runs                         — listar todos los runs
  GET    /runs/{id}                    — estado de un run
  WS     /runs/{id}/logs               — logs en tiempo real (WebSocket)
  GET    /health                       — health check
  
"""

from __future__ import annotations

import asyncio
import uuid

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from orchestrator.api.schemas import (
    CreatePipelineRequest,
    ErrorResponse,
    PipelineResponse,
    RunResponse,
    TriggerRunRequest,
    run_to_response,
)
from orchestrator.api.store import NotFoundError, store
from orchestrator.api.websocket import ws_manager
from orchestrator.core.models import TriggerType
from orchestrator.core.parser import PipelineParseError, PipelineParser
from orchestrator.execution.docker_runner import DockerRunner, DockerRunnerError
from orchestrator.execution.scheduler import Scheduler

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="CI/CD Orchestrator",
    description="Orquestador de pipelines CI/CD con ejecución paralela en Docker",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

parser   = PipelineParser()

# El runner se inicializa lazy para no fallar al importar si Docker no está
_runner: DockerRunner | None = None

def get_runner() -> DockerRunner:
    global _runner
    if _runner is None:
        _runner = DockerRunner()
    return _runner


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health", tags=["Sistema"])
async def health():
    return {"status": "ok", "version": "0.1.0"}


# ---------------------------------------------------------------------------
# Pipelines
# ---------------------------------------------------------------------------

@app.post(
    "/pipelines",
    response_model=PipelineResponse,
    status_code=201,
    tags=["Pipelines"],
    summary="Registrar un pipeline desde YAML",
)
async def create_pipeline(body: CreatePipelineRequest):
    """
    Recibe el contenido YAML de un pipeline, lo valida y lo registra.
    Retorna el ID del pipeline creado.
    """
    try:
        definition = parser.parse_string(body.yaml_content)
    except PipelineParseError as e:
        raise HTTPException(status_code=422, detail=str(e))

    pipeline_id = str(uuid.uuid4())
    record = store.save_pipeline(pipeline_id, definition, body.yaml_content)

    return PipelineResponse(
        id=record["id"],
        name=record["name"],
        description=record["description"],
        created_at=record["created_at"],
    )


@app.get(
    "/pipelines",
    response_model=list[PipelineResponse],
    tags=["Pipelines"],
    summary="Listar todos los pipelines",
)
async def list_pipelines():
    records = store.list_pipelines()
    return [
        PipelineResponse(
            id=r["id"],
            name=r["name"],
            description=r["description"],
            created_at=r["created_at"],
        )
        for r in records
    ]


@app.get(
    "/pipelines/{pipeline_id}",
    response_model=PipelineResponse,
    tags=["Pipelines"],
    summary="Obtener detalle de un pipeline",
)
async def get_pipeline(pipeline_id: str):
    try:
        record = store.get_pipeline(pipeline_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return PipelineResponse(
        id=record["id"],
        name=record["name"],
        description=record["description"],
        created_at=record["created_at"],
    )


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------

@app.post(
    "/pipelines/{pipeline_id}/runs",
    response_model=RunResponse,
    status_code=202,
    tags=["Runs"],
    summary="Disparar una ejecución del pipeline",
)
async def trigger_run(pipeline_id: str, body: TriggerRunRequest):
    """
    Dispara una ejecución del pipeline en background.
    Retorna inmediatamente con el run en estado PENDING/RUNNING.
    Los logs se pueden seguir en tiempo real por WebSocket.
    """
    try:
        record = store.get_pipeline(pipeline_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    definition = record["definition"]

    # Mapear string a TriggerType
    try:
        trigger = TriggerType(body.trigger)
    except ValueError:
        trigger = TriggerType.MANUAL

    # Crear el run inicial en estado PENDING
    from orchestrator.core.models import PipelineRun
    run = PipelineRun(
        pipeline_id=uuid.UUID(pipeline_id),
        trigger=trigger,
        branch=body.branch,
    )
    store.save_run(run)

    # Lanzar la ejecución en background (no bloquea la respuesta HTTP)
    asyncio.create_task(
        _execute_run(run_id=str(run.id), definition=definition)
    )

    return run_to_response(run)


async def _execute_run(run_id: str, definition) -> None:
    """
    Ejecuta el pipeline en background y actualiza el store.
    Transmite logs a los clientes WebSocket conectados.
    """
    try:
        runner    = get_runner()
        scheduler = Scheduler(runner=runner)

        def on_event(event: str, data: dict) -> None:
            """Reenvía eventos relevantes al WebSocket."""
            if event == "log":
                asyncio.create_task(
                    ws_manager.broadcast(run_id, {
                        "type":    "log",
                        "job_key": data["job_key"],
                        "step":    data["step"],
                        "line":    data["line"],
                    })
                )
            elif event in ("job_started", "job_finished"):
                asyncio.create_task(
                    ws_manager.broadcast(run_id, {
                        "type": event,
                        **data,
                    })
                )

        completed_run = await scheduler.execute(
            pipeline=definition,
            on_event=on_event,
        )

        # Actualizar el run en el store con el resultado final
        original = store.get_run(run_id)
        original.status   = completed_run.status
        original.ended_at = completed_run.ended_at
        original.jobs     = completed_run.jobs
        store.save_run(original)

    except DockerRunnerError as e:
        # Docker no disponible — marcar como fallido
        from orchestrator.core.models import RunStatus
        from datetime import datetime
        run = store.get_run(run_id)
        run.status   = RunStatus.FAILED
        run.ended_at = datetime.utcnow()
        store.save_run(run)
        await ws_manager.broadcast(run_id, {
            "type":  "error",
            "error": str(e),
        })
    finally:
        await ws_manager.close_all(run_id)


@app.get(
    "/runs",
    response_model=list[RunResponse],
    tags=["Runs"],
    summary="Listar todos los runs",
)
async def list_runs(pipeline_id: str | None = None):
    runs = store.list_runs(pipeline_id=pipeline_id)
    return [run_to_response(r) for r in runs]


@app.get(
    "/runs/{run_id}",
    response_model=RunResponse,
    tags=["Runs"],
    summary="Estado y resultado de un run",
)
async def get_run(run_id: str):
    try:
        run = store.get_run(run_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return run_to_response(run)


# ---------------------------------------------------------------------------
# WebSocket — logs en tiempo real
# ---------------------------------------------------------------------------

@app.websocket("/runs/{run_id}/logs")
async def run_logs_websocket(run_id: str, websocket: WebSocket):
    """
    Conectar a este WebSocket para recibir logs en tiempo real de un run.

    Mensajes recibidos (JSON):
      {"type": "log",          "job_key": "build", "step": "Compilar", "line": "..."}
      {"type": "job_started",  "job_key": "build", "image": "python:3.11"}
      {"type": "job_finished", "job_key": "build", "status": "success", "duration": 4.2}
      {"type": "run_finished"}
      {"type": "error",        "error": "mensaje de error"}
    """
    # Verificar que el run existe
    try:
        store.get_run(run_id)
    except NotFoundError:
        await websocket.close(code=4004)
        return

    await ws_manager.connect(run_id, websocket)
    try:
        # Mantener la conexión abierta hasta que el cliente desconecte
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(run_id, websocket)
