from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hello_gitsync",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gitsync"]
) as dag:
    say_hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "hello from git-sync - $(date)"'
    )