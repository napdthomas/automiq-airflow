import pytest
from airflow.models import DagBag

def test_no_import_errors():
    dag_bag = DagBag()
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"

def test_dags_load():
    dag_bag = DagBag()
    assert dag_bag.dag_ids, "No DAGs found"
