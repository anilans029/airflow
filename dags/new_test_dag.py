from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

# DAG definition
with DAG(
    dag_id="new_test_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # only trigger manually
    catchup=False,
    tags=["test"],
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    start >> end
