from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

with DAG(
    dag_id='dag-examples',
    start_date=datetime(2024, 10, 9),
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:
    start_task = EmptyOperator(
        task_id='start'
    )
        
    end_task = EmptyOperator(
        task_id='end'
    )
