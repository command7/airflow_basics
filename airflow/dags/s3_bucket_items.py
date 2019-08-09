from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
import logging, datetime


def log_bucket_items():
    s3_hook = S3Hook(aws_conn_id='aws_credentials')
    bucket_name = Variable.get('udacity_s3_bucket')
    bucket_prefix = Variable.get('udacity_s3_prefix')
    bucket_items = s3_hook.list_keys(bucket_name=bucket_name, prefix=bucket_prefix)
    for bucket_item in bucket_items:
        logging.info(f"- s3://{bucket_name}/{bucket_prefix}/{bucket_item}")


bucket_keys_dag = DAG(
    'S3_bucket_items',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1),
    schedule_interval='@daily'
)


list_bucket_items_task = PythonOperator(
    task_id='Log_bucket_keys.task',
    python_callable=log_bucket_items,
    dag=bucket_keys_dag
)