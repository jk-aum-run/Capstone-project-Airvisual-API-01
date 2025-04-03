from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator

with DAG(
    "air_dag_oum",
    start_date=timezone.datetime(2025, 3, 29),
    schedule=None,
    tags=["dpu", "air_dag_oum"],
):
    t1 = EmptyOperator(task_id="t1")
    t2 = EmptyOperator(task_id="t2")

    t1 >> t2

