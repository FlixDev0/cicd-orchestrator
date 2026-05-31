# CI/CD Orchestrator

> Orquestador de pipelines CI/CD con ejecución paralela en contenedores Docker, API REST, logs en tiempo real por WebSocket y dashboard web.

![Python](https://img.shields.io/badge/Python-3.11+-blue?style=flat-square&logo=python)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111-green?style=flat-square&logo=fastapi)
![Docker](https://img.shields.io/badge/Docker-required-blue?style=flat-square&logo=docker)
![SQLite](https://img.shields.io/badge/SQLite-dev-lightgrey?style=flat-square&logo=sqlite)
![Tests](https://img.shields.io/badge/tests-60%20passing-brightgreen?style=flat-square)

---

## ¿Qué es esto?

Un orquestador CI/CD construido desde cero para entender en profundidad cómo funcionan los sistemas de integración y despliegue continuo. Permite definir pipelines en archivos YAML, ejecutarlos en contenedores Docker aislados con paralelismo real, y monitorear el progreso en tiempo real desde un dashboard web.

**No usa Jenkins, GitHub Actions ni ningún orquestador existente** — todo está implementado desde cero: el parser YAML, el motor de grafos DAG, el scheduler de ejecución paralela, el runner de Docker, la API REST y el dashboard.

---

## Arquitectura

```
┌─────────────────────────────────────────────────────────┐
│                    Dashboard Web                         │
│         HTML + JS + WebSocket (tiempo real)              │
└────────────────────┬────────────────────────────────────┘
                     │ HTTP / WebSocket
┌────────────────────▼────────────────────────────────────┐
│                   API REST (FastAPI)                     │
│   POST /pipelines  │  POST /runs  │  WS /runs/{id}/logs │
└────────────────────┬────────────────────────────────────┘
                     │
         ┌───────────▼───────────┐
         │    Núcleo del sistema  │
         │  ┌─────────────────┐  │
         │  │   Parser YAML   │  │  Lee y valida pipeline.yml
         │  └────────┬────────┘  │
         │  ┌────────▼────────┐  │
         │  │   DAG Engine    │  │  Resuelve dependencias entre jobs
         │  └────────┬────────┘  │
         │  ┌────────▼────────┐  │
         │  │   Scheduler     │  │  Ejecución paralela por olas
         │  └────────┬────────┘  │
         │  ┌────────▼────────┐  │
         │  │  Docker Runner  │  │  Crea y ejecuta contenedores
         │  └─────────────────┘  │
         └───────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│              Persistencia (SQLAlchemy)                   │
│         SQLite (desarrollo) / PostgreSQL (prod)          │
└─────────────────────────────────────────────────────────┘
```

### Componentes principales

| Módulo | Archivo | Responsabilidad |
|--------|---------|-----------------|
| Parser | `orchestrator/core/parser.py` | Lee y valida archivos `pipeline.yml` |
| DAG Engine | `orchestrator/core/dag.py` | Grafo de dependencias, detección de ciclos |
| Scheduler | `orchestrator/execution/scheduler.py` | Ejecución paralela por olas con asyncio |
| Docker Runner | `orchestrator/execution/docker_runner.py` | Crea contenedores, ejecuta steps, captura logs |
| API REST | `orchestrator/api/app.py` | Endpoints HTTP + WebSocket |
| Store DB | `orchestrator/storage/db_store.py` | Persistencia con SQLAlchemy |
| Dashboard | `dashboard/index.html` | UI web con logs en tiempo real |

---

## Sintaxis del pipeline YAML

```yaml
name: mi-pipeline
description: "Pipeline de ejemplo"

on:
  push:
    branches: [main, develop]
  manual: true

env:
  APP_ENV: production

jobs:
  build:
    image: python:3.11-slim
    steps:
      - name: Instalar dependencias
        run: pip install -r requirements.txt
      - name: Compilar
        run: python setup.py build

  lint:
    image: python:3.11-slim
    steps:
      - name: Análisis estático
        run: ruff check .

  test:
    image: python:3.11-slim
    needs: [build, lint]       # espera que build Y lint terminen
    steps:
      - name: Tests
        run: pytest tests/ --tb=short
        timeout: 300

  deploy:
    image: alpine:3.18
    needs: [test]
    condition: "branch == 'main'"
    steps:
      - name: Deploy
        run: ./scripts/deploy.sh
```

---

## Instalación y uso

### Requisitos

- Python 3.11+
- Docker Desktop (corriendo)
- Git

### Instalación local

```bash
# 1. Clonar el repositorio
git clone https://github.com/TU_USUARIO/cicd-orchestrator.git
cd cicd-orchestrator

# 2. Crear entorno virtual
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # Linux/Mac

# 3. Instalar dependencias
pip install fastapi uvicorn sqlalchemy pydantic pydantic-settings \
            PyYAML networkx docker websockets cryptography \
            httpx python-multipart pytest pytest-asyncio

# 4. Levantar el servidor
python main.py
```

Abrir en el navegador: **http://localhost:8000**

### Con Docker Compose

```bash
docker-compose up
```

Abrir en el navegador: **http://localhost:8000**

---

## Uso del dashboard

1. **Nuevo Pipeline** → pega o escribe tu `pipeline.yml` → **Registrar pipeline**
2. **Ejecutar pipeline** → elige rama y trigger → **▶ Ejecutar**
3. El dashboard redirige automáticamente al detalle del run
4. Los logs aparecen en tiempo real por WebSocket
5. Cada job muestra su estado (success / failed / skipped) y duración

---

## API REST

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| `GET` | `/health` | Estado del servidor |
| `POST` | `/pipelines` | Registrar pipeline desde YAML |
| `GET` | `/pipelines` | Listar pipelines |
| `GET` | `/pipelines/{id}` | Detalle de un pipeline |
| `POST` | `/pipelines/{id}/runs` | Disparar ejecución |
| `GET` | `/runs` | Listar runs |
| `GET` | `/runs/{id}` | Estado y resultado de un run |
| `WS` | `/runs/{id}/logs` | Logs en tiempo real |

Documentación interactiva disponible en: **http://localhost:8000/docs**

### Ejemplo con curl

```bash
# Crear pipeline
curl -X POST http://localhost:8000/pipelines \
  -H "Content-Type: application/json" \
  -d '{"yaml_content": "name: test\njobs:\n  build:\n    image: python:3.11-slim\n    steps:\n      - run: echo ok"}'

# Disparar run
curl -X POST http://localhost:8000/pipelines/{ID}/runs \
  -H "Content-Type: application/json" \
  -d '{"branch": "main", "trigger": "manual"}'

# Consultar estado
curl http://localhost:8000/runs/{RUN_ID}
```

---

## Tests

```bash
# Todos los tests
pytest tests/ -v

# Por módulo
pytest tests/test_core.py        -v   # Parser y DAG Engine
pytest tests/test_docker_runner.py -v  # Docker Runner
pytest tests/test_scheduler.py    -v   # Scheduler
pytest tests/test_db_store.py     -v   # Persistencia
pytest tests/test_api.py          -v   # API REST
```

**60 tests pasando** — cobertura de todos los módulos del sistema.

---

## Variables de entorno

| Variable | Default | Descripción |
|----------|---------|-------------|
| `DATABASE_URL` | `sqlite:///./cicd_orchestrator.db` | URL de conexión a la DB |
| `HOST` | `0.0.0.0` | Host del servidor |
| `PORT` | `8000` | Puerto del servidor |

Para usar PostgreSQL en producción:

```bash
# Linux/Mac
export DATABASE_URL="postgresql://user:pass@localhost/cicd_db"

# Windows PowerShell
$env:DATABASE_URL = "postgresql://user:pass@localhost/cicd_db"
```

---

## Estructura del proyecto

```
cicd-orchestrator/
├── orchestrator/
│   ├── core/
│   │   ├── models.py        # Modelos de dominio (Pydantic)
│   │   ├── parser.py        # Parser de pipeline.yml
│   │   └── dag.py           # Motor DAG
│   ├── execution/
│   │   ├── docker_runner.py # Ejecución en contenedores Docker
│   │   └── scheduler.py     # Coordinación paralela
│   ├── api/
│   │   ├── app.py           # FastAPI — endpoints y WebSocket
│   │   ├── schemas.py       # Schemas request/response
│   │   └── websocket.py     # Manager de conexiones WS
│   └── storage/
│       ├── database.py      # Conexión SQLAlchemy
│       ├── db_store.py      # Store con persistencia
│       └── orm_models.py    # Modelos ORM (tablas)
├── dashboard/
│   └── index.html           # Dashboard web
├── tests/
│   ├── test_core.py
│   ├── test_docker_runner.py
│   ├── test_scheduler.py
│   ├── test_db_store.py
│   └── test_api.py
├── examples/
│   └── pipeline.yml         # Pipeline de ejemplo
├── main.py                  # Punto de entrada
├── docker-compose.yml       # Despliegue con Docker
└── requirements.txt
```

---

## Decisiones técnicas

**¿Por qué asyncio + ThreadPoolExecutor?**
Docker SDK es síncrono. Para no bloquear el event loop de FastAPI, todas las operaciones de Docker corren en threads del executor. Los logs se reenvían al loop principal con `loop.call_soon_threadsafe()`.

**¿Por qué DAG y no lista ordenada?**
Un DAG permite expresar dependencias complejas (job A espera a B y C) y detectar ciclos antes de ejecutar. Permite también calcular qué jobs pueden correr en paralelo en cada momento.

**¿Por qué SQLite en desarrollo?**
Cero configuración, perfecto para desarrollo local. La misma interfaz (`DatabaseStore`) funciona con PostgreSQL en producción cambiando solo `DATABASE_URL`.

---

## Autor

Juan manuel pizarro - Estudiante de Ingeniería en Informática
Desarrollado con Python, FastAPI, SQLAlchemy, Docker SDK y asyncio.
