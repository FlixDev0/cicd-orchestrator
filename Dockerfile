# ── Build stage ─────────────────────────────────────────────────────────────
FROM python:3.16-slim AS base

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar e instalar dependencias Python
COPY requirements-docker.txt .
RUN pip install --no-cache-dir -r requirements-docker.txt

# Copiar el código fuente
COPY orchestrator/ ./orchestrator/
COPY dashboard/    ./dashboard/
COPY main.py       .

# Crear directorio para la base de datos
RUN mkdir -p /app/data

# Exponer puerto
EXPOSE 8000

# Variables de entorno por defecto
ENV DATABASE_URL=sqlite:///./data/cicd_orchestrator.db
ENV PYTHONPATH=/app

# Comando de inicio
CMD ["python", "main.py"]
