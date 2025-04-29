from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3

AWS_BUCKET_NAME = "transformed"
AWS_REGION = "us-east-1"
SOURCE_PREFIX = "odw/"
ARCHIVE_PREFIX = "odw/archive/"

def archive_loaded_files():
    s3 = boto3.client('s3', region_name=AWS_REGION)
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=AWS_BUCKET_NAME, Prefix=SOURCE_PREFIX)
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if "archive/" not in key and key.endswith(".parquet"):
                copy_source = {'Bucket': AWS_BUCKET_NAME, 'Key': key}
                s3.copy_object(Bucket=AWS_BUCKET_NAME, CopySource=copy_source, Key=key.replace(SOURCE_PREFIX, ARCHIVE_PREFIX))
                s3.delete_object(Bucket=AWS_BUCKET_NAME, Key=key)

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='odw_snowflake_loader',
    default_args=default_args,
    description='Load ODW Parquet files into Snowflake after ODW-ingestion',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['odw', 'snowflake', 's3'],
) as dag:

    wait_for_odw_ingestion = ExternalTaskSensor(
        task_id='wait_for_odw_ingestion',
        external_dag_id='ODW-ingestion',
        external_task_id=None,
        allowed_states=['success'],
        timeout=3600,
        poke_interval=60,
        mode='poke'
    )

    load_parquet_to_snowflake = SnowflakeOperator(
        task_id='load_parquet_to_snowflake',
        snowflake_conn_id='your_snowflake_conn',
        sql="CALL raw.sp_load_parquet_from_s3_enhanced();",
    )

    archive_files_in_s3 = PythonOperator(
        task_id='archive_files_in_s3',
        python_callable=archive_loaded_files,
    )

    wait_for_odw_ingestion >> load_parquet_to_snowflake >> archive_files_in_s3