from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

def decide_child_dag(**kwargs):
    # Read variable "task_to_run"
    task_to_run = Variable.get("task_to_run", default_var="task1")

    # Map task → child DAG ID
    mapping = {
        "task1": "child_dag_1",
        "task2": "child_dag_2",
        "task3": "child_dag_3",
        "task4": "child_dag_4",
        "task5": "child_dag_5",
        "task6": "child_dag_6",
        "task7": "child_dag_7",
    }

    # Pick which TriggerDagRunOperator to follow
    return f"trigger_{mapping[task_to_run]}"

with DAG(
    dag_id="parent_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,   # Only runs on trigger
    catchup=False,
    default_args=default_args,
) as dag:

    # Step 1: Decide which DAG to run
    choose_dag = BranchPythonOperator(
        task_id="choose_dag",
        python_callable=decide_child_dag,
        provide_context=True,
    )

    # Step 2: Create trigger operators for each possible DAG
    trigger_child_dag1 = TriggerDagRunOperator(
        task_id="trigger_child_dag_1",
        trigger_dag_id="child_dag_1",
    )

    trigger_child_dag2 = TriggerDagRunOperator(
        task_id="trigger_child_dag_2",
        trigger_dag_id="child_dag_2",
    )

    trigger_child_dag3 = TriggerDagRunOperator(
        task_id="trigger_child_dag_3",
        trigger_dag_id="child_dag_3",
    )

    trigger_child_dag4 = TriggerDagRunOperator(
        task_id="trigger_child_dag_4",
        trigger_dag_id="child_dag_4",
    )

    trigger_child_dag5 = TriggerDagRunOperator(
        task_id="trigger_child_dag_5",
        trigger_dag_id="child_dag_5",
    )

    trigger_child_dag6 = TriggerDagRunOperator(
        task_id="trigger_child_dag_6",
        trigger_dag_id="child_dag_6",
    )

    trigger_child_dag7 = TriggerDagRunOperator(
        task_id="trigger_child_dag_7",
        trigger_dag_id="child_dag_7",
    )
    

    # DAG flow
    choose_dag >> [
        trigger_child_dag1,
        trigger_child_dag2,
        trigger_child_dag3,
        trigger_child_dag4,
        trigger_child_dag5,
        trigger_child_dag6,
        trigger_child_dag7,
    ]
