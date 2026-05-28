"""
Punto de entrada del servidor.
Ejecutar con: python main.py
O con uvicorn: uvicorn orchestrator.api.app:app --reload
"""

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "orchestrator.api.app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,       # recarga automática al editar archivos
    )
