import importlib
import sys
from contextlib import ExitStack
from pathlib import Path

import pytest
from pyspark.pipelines import graph_element_registry
from pyspark.sql import SparkSession

TESTS_ROOT = Path(__file__).resolve().parent
PACKAGE_ROOT = TESTS_ROOT.parent

if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))


class _PassThroughRegistry(graph_element_registry.GraphElementRegistry):
    """Minimal registry that lets the `@dp.table` / `@dp.materialized_view`
    decorators run outside a Databricks pipeline so modules can be imported for
    unit testing. Decorated functions are registered but do nothing here."""

    def register_output(self, output):
        pass

    def register_flow(self, flow):
        pass

    def register_auto_cdc_flow(self, flow):
        pass

    def register_sql(self, sql_text, file_path):
        pass


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("transformations-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        # Databricks Runtime defaults ANSI mode to off (unlike OSS Spark 4.x),
        # where to_timestamp('') yields NULL rather than throwing.
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def registry():
    return _PassThroughRegistry()


def load_transformations_module(name):
    """Import a `transformations.*` module inside a pipeline registration context so
    the declarations (decorators) execute without error. Returns the module."""
    with graph_element_registry.graph_element_registration_context(_PassThroughRegistry()):
        return importlib.import_module(name)
