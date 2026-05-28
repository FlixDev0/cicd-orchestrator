"""
WebSocket Manager — transmite logs en tiempo real a clientes conectados.

Cuando un run está en ejecución, cualquier cliente puede conectarse al
WebSocket de ese run y recibir cada línea de log al instante.

Arquitectura:
  - Por cada run_id hay una lista de WebSockets conectados.
  - El Scheduler emite eventos "log" que el manager reenvía a todos.
  - Cuando el run termina, el manager cierra las conexiones.
"""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict

from fastapi import WebSocket


class ConnectionManager:
    """
    Gestiona las conexiones WebSocket activas por run_id.
    """

    def __init__(self) -> None:
        # run_id → lista de WebSockets conectados
        self._connections: dict[str, list[WebSocket]] = defaultdict(list)

    async def connect(self, run_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        self._connections[run_id].append(websocket)

    def disconnect(self, run_id: str, websocket: WebSocket) -> None:
        connections = self._connections.get(run_id, [])
        if websocket in connections:
            connections.remove(websocket)

    async def broadcast(self, run_id: str, message: dict) -> None:
        """Envía un mensaje JSON a todos los clientes del run."""
        dead: list[WebSocket] = []
        for ws in list(self._connections.get(run_id, [])):
            try:
                await ws.send_text(json.dumps(message))
            except Exception:
                dead.append(ws)

        # Limpiar conexiones muertas
        for ws in dead:
            self.disconnect(run_id, ws)

    async def close_all(self, run_id: str) -> None:
        """Cierra todas las conexiones de un run cuando termina."""
        for ws in list(self._connections.get(run_id, [])):
            try:
                await ws.send_text(json.dumps({"type": "run_finished"}))
                await ws.close()
            except Exception:
                pass
        self._connections.pop(run_id, None)

    def active_connections(self, run_id: str) -> int:
        return len(self._connections.get(run_id, []))


# Instancia global
ws_manager = ConnectionManager()
