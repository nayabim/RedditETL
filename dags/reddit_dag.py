from datetime import datetime
import os
import sys
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from airflow.operators.python import PythonOperator
from include.pipelines.reddit_etl_pipeline import reddit_pipeline
from include.pipelines.s3_upload_pipeline import upload_s3_pipeline

default_args = {
    'owner': "Nayab Imtiaz",
    'start_date': datetime(2023, 10, 22),
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'reddit']
)

extract = PythonOperator(
    task_id='reddit_extraction',
    python_callable=reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'dataengineering',
        'time_filter': 'day',
        'limit': 100
    },
    dag=dag
)

uplaod_to_s3 = PythonOperator(
    task_id='reddit_to_s3',
    python_callable=upload_s3_pipeline,
    dag=dag
)


extract >> uplaod_to_s3