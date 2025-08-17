from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


def print_hello_world():
    print("Hello, world!")

# DAG definition
with DAG(
    dag_id="new_test_dag",
    start_date=datetime(2023, 1, 1),
    
    schedule_interval=None,  # only trigger manually
    catchup=False,
    tags=["test"],
) as dag:


    task1 = PythonOperator(task_id='print_hello_world', python_callable=print_hello_world)
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    start >> task1 >> end
    