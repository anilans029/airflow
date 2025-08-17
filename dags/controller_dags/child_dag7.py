from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="child_dag_7",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_task = BashOperator(
        task_id="child_task",
        bash_command="echo 'Running Child DAG 7 job!'"
    )
