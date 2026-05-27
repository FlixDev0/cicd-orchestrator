"""
DAG Engine — Motor de resolución de dependencias.

Toma un PipelineDefinition y construye un grafo dirigido acíclico (DAG)
que representa el orden de ejecución de los jobs. Detecta ciclos,
calcula qué jobs pueden correr en paralelo y cuáles deben esperar.

Ejemplo de pipeline:
    lint ──┐
           ├──► deploy
    test ──┘

El DAG engine devuelve: [[lint, test], [deploy]]
(lint y test en paralelo, deploy espera a ambos)
"""

from __future__ import annotations

from typing import Generator

import networkx as nx

from orchestrator.core.models import PipelineDefinition


class DAGError(Exception):
    """Se lanza cuando el grafo tiene un ciclo o está mal formado."""
    pass


class DAGEngine:
    """
    Construye y analiza el DAG de ejecución de un pipeline.

    Uso:
        engine = DAGEngine(pipeline)
        for wave in engine.execution_waves():
            # 'wave' es una lista de job_keys que pueden correr en paralelo
            run_in_parallel(wave)
    """

    def __init__(self, pipeline: PipelineDefinition) -> None:
        self.pipeline = pipeline
        self._graph  = self._build_graph()

    # ------------------------------------------------------------------
    # Construcción del grafo
    # ------------------------------------------------------------------

    def _build_graph(self) -> nx.DiGraph:
        graph = nx.DiGraph()

        # Agregar todos los nodos primero
        for job_key in self.pipeline.jobs:
            graph.add_node(job_key)

        # Agregar aristas: "A needs B" significa B → A (B debe terminar antes que A)
        for job_key, job in self.pipeline.jobs.items():
            for dependency in job.needs:
                graph.add_edge(dependency, job_key)

        # Detectar ciclos — un ciclo haría el pipeline imposible de ejecutar
        cycles = list(nx.simple_cycles(graph))
        if cycles:
            cycle_str = " → ".join(cycles[0] + [cycles[0][0]])
            raise DAGError(
                f"El pipeline tiene un ciclo de dependencias: {cycle_str}"
            )

        return graph

    # ------------------------------------------------------------------
    # Consultas sobre el DAG
    # ------------------------------------------------------------------

    def execution_waves(self) -> list[list[str]]:
        """
        Retorna los jobs agrupados en 'olas' de ejecución paralela.
        Cada ola contiene los jobs que pueden correr simultáneamente.

        Ejemplo: [[lint, build], [test], [deploy]]
        - Ola 1: lint y build corren en paralelo (sin dependencias)
        - Ola 2: test corre cuando lint Y build terminaron
        - Ola 3: deploy corre cuando test terminó
        """
        waves: list[list[str]] = []
        remaining = set(self.pipeline.job_names())
        completed: set[str] = set()

        while remaining:
            # Jobs que pueden correr ahora: todos sus deps ya están completados
            ready = [
                job_key for job_key in remaining
                if all(dep in completed for dep in self._graph.predecessors(job_key))
            ]

            if not ready:
                # No hay jobs listos pero quedan pendientes → ciclo no detectado antes
                raise DAGError(
                    f"No se puede resolver el orden de ejecución. "
                    f"Jobs bloqueados: {remaining}"
                )

            waves.append(sorted(ready))   # sorted para ejecución determinista
            completed.update(ready)
            remaining.difference_update(ready)

        return waves

    def dependencies_of(self, job_key: str) -> list[str]:
        """Retorna los jobs que deben terminar antes que job_key."""
        return list(self._graph.predecessors(job_key))

    def dependents_of(self, job_key: str) -> list[str]:
        """Retorna los jobs que esperan que job_key termine."""
        return list(self._graph.successors(job_key))

    def topological_order(self) -> list[str]:
        """
        Retorna todos los jobs en un orden válido de ejecución (secuencial).
        Útil para visualización y debugging.
        """
        return list(nx.topological_sort(self._graph))

    def can_run(self, job_key: str, completed_jobs: set[str]) -> bool:
        """True si todas las dependencias de job_key ya están completadas."""
        return all(dep in completed_jobs for dep in self._graph.predecessors(job_key))

    def summary(self) -> dict:
        """Resumen del DAG para logging y API."""
        return {
            "total_jobs":   len(self.pipeline.jobs),
            "total_edges":  self._graph.number_of_edges(),
            "waves":        self.execution_waves(),
            "topo_order":   self.topological_order(),
        }
