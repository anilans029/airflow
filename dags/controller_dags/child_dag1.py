from pdb import run
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

default_args = {
    'owner': 'anil',
    'email': ['anilsai029@gmail.com'],
    'email_on_failure': True,
    'email_on_sla_miss': True
}

def dag_1_task_simulator():
    print("executing the dag 1 task.....")
    time.sleep(90)
    print("completed executing the dag 1 task.....")
    

with DAG(
    dag_id="child_dag_1",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_task = BashOperator(
        task_id="child_task",
        bash_command="echo 'Running Child DAG 1 job!'"
    )

    dag_1_task = PythonOperator(
        task_id="dag_1_task",
        python_callable=dag_1_task_simulator,
        sla=timedelta(seconds=30)
    )

    run_task >> dag_1_task