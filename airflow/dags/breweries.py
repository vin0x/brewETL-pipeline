import sys

# adding the .scripts path for ETL part
sys.path.append('/opt/airflow/scripts')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from extract import extract_data
from transform import transform_data
from aggregate import aggregate_data

# setting email on failure and automatically retry with 5 minutes delay
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['vinigoes@outlook.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'breweries_data_pipeline',
    default_args=default_args,
    description='Pipeline to extract breweries data from Open Breweries public API',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 10, 1),
    catchup=False,
) as dag:

    task_extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    task_aggregate = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data,
    )

    task_extract >> task_transform >> task_aggregate
