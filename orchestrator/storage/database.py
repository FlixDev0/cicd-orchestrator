"""
Conexión a la base de datos y gestión de sesiones.

- En desarrollo: SQLite (archivo local, sin configuración extra)
- En producción: PostgreSQL (configurado via variable de entorno DATABASE_URL)

Uso:
    from orchestrator.storage.database import get_session, init_db

    # Inicializar tablas al arrancar la app
    init_db()

    # Obtener una sesión (context manager)
    with get_session() as session:
        session.add(registro)
        session.commit()
"""

from __future__ import annotations
import os
from contextlib import contextmanager
from typing import Generator
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from orchestrator.storage.orm_models import Base

# ---------------------------------------------------------------------------
# Configuración de la URL de conexión
# ---------------------------------------------------------------------------

def get_database_url() -> str:
    """
    Lee la URL de la DB desde la variable de entorno DATABASE_URL.
    Si no está definida, usa SQLite local (desarrollo).
    """
    url = os.getenv("DATABASE_URL", "sqlite:///./cicd_orchestrator.db")
    return url


# ---------------------------------------------------------------------------
# Engine y SessionFactory
# ---------------------------------------------------------------------------

def create_db_engine(database_url: str | None = None):
    url = database_url or get_database_url()

    if url.startswith("sqlite"):
        # SQLite necesita check_same_thread=False para funcionar con FastAPI
        engine = create_engine(
            url,
            connect_args={"check_same_thread": False},
            echo=False,   # True para ver las queries SQL en consola (debug)
        )
        # Activar foreign keys en SQLite (no están activas por defecto)
        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(conn, _):
            conn.execute("PRAGMA foreign_keys=ON")
    else:
        engine = create_engine(url, echo=False)

    return engine


# Engine global
_engine = None
_SessionFactory = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_db_engine()
    return _engine


def get_session_factory():
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(
            bind=get_engine(),
            autocommit=False,
            autoflush=False,
        )
    return _SessionFactory


# ---------------------------------------------------------------------------
# Inicialización de tablas
# ---------------------------------------------------------------------------

def init_db(database_url: str | None = None) -> None:
    """
    Crea todas las tablas en la base de datos si no existen.
    Llamar una vez al arrancar la aplicación.
    """
    engine = create_db_engine(database_url) if database_url else get_engine()
    Base.metadata.create_all(bind=engine)


# ---------------------------------------------------------------------------
# Context manager para sesiones
# ---------------------------------------------------------------------------

@contextmanager
def get_session() -> Generator[Session, None, None]:
    """
    Retorna una sesión de base de datos como context manager.
    Hace commit automático al salir sin errores, rollback si hay excepción.

    Uso:
        with get_session() as session:
            session.add(objeto)
            # commit automático al salir del with
    """
    SessionFactory = get_session_factory()
    session = SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
