from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="weather",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="say_hi")
