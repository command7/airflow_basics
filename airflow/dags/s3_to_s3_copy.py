from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
import datetime, logging


def copy_contents_to_local():
    s3_hook = S3Hook(aws_conn_id='aws_credentials')
    udacity_bucket = Variable.get('udacity_s3_bucket')
    udacity_bucket_prefix = Variable.get('udacity_s3_prefix')
    my_bucket = Variable.get('my_s3_bucket')
    object_keys = s3_hook.list_keys(bucket_name=udacity_bucket,
                                    prefix=udacity_bucket_prefix)
    for object_key in object_keys:
        if object_key.endswith('csv'):
            destination_filename = object_key.split('/')[-1]
            logging.info(f'Copying {object_key}')
            s3_hook.copy_object(source_bucket_name=udacity_bucket,
                                source_bucket_key=object_key,
                                dest_bucket_key=destination_filename,
                                dest_bucket_name=my_bucket)
            logging.info(f'{object_key} copied as {destination_filename}')


copy_dag = DAG(
    'Copy_data_between_s3buckets',
    start_date=datetime.datetime.now()
)


copy_task = PythonOperator(
    task_id='Copy_s3_to_s3.task',
    python_callable=copy_contents_to_local,
    dag=copy_dag
)