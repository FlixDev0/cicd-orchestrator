"""
Tests del Parser y DAG Engine.
Ejecutar con: pytest tests/test_core.py -v
"""

import pytest
from orchestrator.core.dag import DAGEngine, DAGError
from orchestrator.core.models import PipelineDefinition
from orchestrator.core.parser import PipelineParseError, PipelineParser

PARSER = PipelineParser()


# ---------------------------------------------------------------------------
# Tests del Parser
# ---------------------------------------------------------------------------

class TestParser:

    def test_parse_pipeline_valido(self):
        yaml = """
name: test-pipeline
jobs:
  build:
    image: python:3.11-slim
    steps:
      - name: Compilar
        run: echo ok
"""
        pipeline = PARSER.parse_string(yaml)
        assert pipeline.name == "test-pipeline"
        assert "build" in pipeline.jobs
        assert len(pipeline.jobs["build"].steps) == 1

    def test_parse_env_global_y_local(self):
        yaml = """
name: test
env:
  GLOBAL: "valor_global"
jobs:
  job1:
    image: python:3.11-slim
    env:
      LOCAL: "valor_local"
    steps:
      - run: echo ok
"""
        pipeline = PARSER.parse_string(yaml)
        merged = pipeline.merge_env("job1")
        assert merged["GLOBAL"] == "valor_global"
        assert merged["LOCAL"] == "valor_local"

    def test_parse_necesidades(self):
        yaml = """
name: test
jobs:
  a:
    image: alpine
    steps:
      - run: echo a
  b:
    image: alpine
    needs: [a]
    steps:
      - run: echo b
"""
        pipeline = PARSER.parse_string(yaml)
        assert pipeline.jobs["b"].needs == ["a"]

    def test_error_pipeline_sin_jobs(self):
        yaml = "name: vacio\njobs: {}"
        with pytest.raises(PipelineParseError, match="al menos un job"):
            PARSER.parse_string(yaml)

    def test_error_step_sin_run(self):
        yaml = """
name: test
jobs:
  job1:
    image: alpine
    steps:
      - name: Sin run
"""
        with pytest.raises(PipelineParseError, match="run"):
            PARSER.parse_string(yaml)

    def test_error_needs_inexistente(self):
        yaml = """
name: test
jobs:
  job1:
    image: alpine
    needs: [job_que_no_existe]
    steps:
      - run: echo ok
"""
        with pytest.raises(PipelineParseError, match="no existe"):
            PARSER.parse_string(yaml)


# ---------------------------------------------------------------------------
# Tests del DAG Engine
# ---------------------------------------------------------------------------

class TestDAGEngine:

    def _pipeline(self, jobs_config: dict) -> PipelineDefinition:
        """Helper: crea un pipeline desde un dict de jobs."""
        yaml_jobs = ""
        for name, needs in jobs_config.items():
            needs_str = str(needs).replace("'", '"')
            yaml_jobs += f"""
  {name}:
    image: alpine
    needs: {needs_str}
    steps:
      - run: echo {name}
"""
        yaml = f"name: test\njobs:\n{yaml_jobs}"
        return PARSER.parse_string(yaml)

    def test_sin_dependencias_una_sola_ola(self):
        pipeline = self._pipeline({"a": [], "b": [], "c": []})
        engine = DAGEngine(pipeline)
        waves = engine.execution_waves()
        assert len(waves) == 1
        assert sorted(waves[0]) == ["a", "b", "c"]

    def test_cadena_lineal_tres_olas(self):
        pipeline = self._pipeline({"a": [], "b": ["a"], "c": ["b"]})
        engine = DAGEngine(pipeline)
        waves = engine.execution_waves()
        assert waves == [["a"], ["b"], ["c"]]

    def test_paralelo_luego_convergencia(self):
        # lint y build en paralelo → test espera a ambos
        pipeline = self._pipeline({
            "lint":  [],
            "build": [],
            "test":  ["lint", "build"],
        })
        engine = DAGEngine(pipeline)
        waves = engine.execution_waves()
        assert sorted(waves[0]) == ["build", "lint"]
        assert waves[1] == ["test"]

    def test_detecta_ciclo(self):
        yaml = """
name: test
jobs:
  a:
    image: alpine
    needs: [b]
    steps:
      - run: echo a
  b:
    image: alpine
    needs: [a]
    steps:
      - run: echo b
"""
        pipeline = PARSER.parse_string(yaml)
        with pytest.raises(DAGError, match="ciclo"):
            DAGEngine(pipeline)

    def test_can_run_con_deps_completadas(self):
        pipeline = self._pipeline({"a": [], "b": ["a"]})
        engine = DAGEngine(pipeline)
        assert engine.can_run("b", completed_jobs={"a"}) is True
        assert engine.can_run("b", completed_jobs=set()) is False

    def test_summary_keys(self):
        pipeline = self._pipeline({"a": [], "b": ["a"]})
        engine = DAGEngine(pipeline)
        summary = engine.summary()
        assert "waves" in summary
        assert "total_jobs" in summary
        assert summary["total_jobs"] == 2
