from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
import datetime, logging


def copy_contents_to_local():
    # s3_hook = S3Hook(aws_conn_id='aws_credentials')
    udacity_bucket = Variable.get('udacity_s3_bucket')
    udacity_bucket_prefix = Variable.get('udacity_s3_prefix')
    my_bucket = Variable.get('my_s3_bucket')
    logging.info(f'Udacity bucket name is {udacity_bucket}/{udacity_bucket_prefix}')
    logging.info(f'My bucket name is {my_bucket}')


copy_dag = DAG(
    'Copy_data_between_s3buckets',
    start_date=datetime.datetime.now()
)


copy_task = PythonOperator(
    task_id='Copy_s3_to_s3.task',
    python_callable=copy_contents_to_local,
    dag=copy_dag
)