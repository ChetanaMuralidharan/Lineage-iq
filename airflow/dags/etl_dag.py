import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Make project code importable inside Airflow container
sys.path.insert(0, '/opt/airflow/project')

default_args = {
    'owner': 'lineageiq',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def run_etl():
    # Import here so it only loads when the task actually runs
    from etl.load_to_snowflake import run_full_etl
    run_full_etl()

with DAG(
    dag_id='lineageiq_etl',
    description='Extract from Postgres, stage to Snowflake internal stage, merge into RAW tables',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=['lineageiq', 'etl', 'raw'],
) as dag:

    etl_task = PythonOperator(
        task_id='extract_load_snowflake',
        python_callable=run_etl,
    )