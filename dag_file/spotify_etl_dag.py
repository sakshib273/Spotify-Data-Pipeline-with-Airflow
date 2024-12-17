from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
import os
import sys

# Adjust the system path to import your run_etl function
sys.path.append(os.path.expanduser('~/'))

# Import the run_etl function from your project
from spotify_project.etl_scripts.spotify_etl import run_etl

# Define the local time zone (Asia/Kolkata)
local_tz = pendulum.timezone('Asia/Kolkata')

default_args = {
    'depends_on_past': False,
    'start_date': local_tz.convert(datetime(2024, 10, 22, 0, 0)),  # Set to your desired start date in local time
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'spotify_etl_dag',
    default_args=default_args,
    description='A DAG to fetch Spotify data and store it in S3',
    schedule_interval='0 11,18,23 * * *',  # Run at 11 AM, 6 PM, and 11 PM IST
    catchup=True, 
)

# Define the task
run_etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl,
    dag=dag,
)

run_etl_task
