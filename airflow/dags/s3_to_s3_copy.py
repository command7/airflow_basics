from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import datetime, logging


create_trips_table_sql = """
CREATE TABLE IF NOT EXISTS TRIPS (
    trip_id INTEGER NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    bikeid INTEGER NOT NULL,
    tripduration DECIMAL(16, 2) NOT NULL,
    from_station_id INTEGER NOT NULL,
    from_station_name VARCHAR(100) NOT NULL,
    to_station_id INTEGER NOT NULL,
    to_station_name VARCHAR(100) NOT NULL,
    usertype VARCHAR(20) NOT NULL,
    gender VARCHAR(6) NOT NULL,
    birthyear SMALLINT NOT NULL,
    PRIMARY KEY(trip_id))
    DISTSTYLE ALL;
"""


copy_all_trips_sql = """
COPY {}
FROM '{}'
ACCESS_KEY_ID '{}'
SECRET_ACCESS_KEY '{}'
IGNOREHEADER 1
DELIMITER ','
"""

def move_data_to_archive_bucket():
    s3_hook = S3Hook(aws_conn_id='aws_credentials')
    data_bucket = Variable.get('bikeshare_etl_bucket')
    unprocessed_data_prefix = Variable.get('bikeshare_source_prefix')
    archive_bucket_prefix = Variable.get('bikeshare_archive_prefix')
    object_keys = s3_hook.list_keys(bucket_name=data_bucket,
                                    prefix=unprocessed_data_prefix)
    for object_key in object_keys:
        logging.info(object_key)
        if object_key.endswith('csv'):
            destination_filename = archive_bucket_prefix + '/' + object_key[17:]
            logging.info(f'Destination : {destination_filename}')
            logging.info(f'Copying {object_key}')
            s3_hook.copy_object(source_bucket_name=data_bucket,
                                source_bucket_key=object_key,
                                dest_bucket_key=destination_filename,
                                dest_bucket_name=data_bucket)
            logging.info(f'{object_key} copied as {destination_filename}')


def validate_s3_t0_s3_copy():
    missing_files = list()
    s3_hook = S3Hook(aws_conn_id='aws_credentials')
    udacity_bucket = Variable.get('udacity_s3_bucket')
    udacity_bucket_prefix = Variable.get('udacity_s3_prefix')
    my_bucket = Variable.get('my_s3_bucket')

    udacity_object_keys = s3_hook.list_keys(bucket_name=udacity_bucket,
                                            prefix=udacity_bucket_prefix)

    for source_object_key in udacity_object_keys:
        if source_object_key.endswith('csv'):
            key_to_check = source_object_key[15:]
            logging.info(f'Checking if {key_to_check} exists.')
            test = s3_hook.check_for_key(key=key_to_check,
                                         bucket_name=my_bucket)
            if test:
                logging.info(f'File: {key_to_check} has been successfully copied.')
            else:
                missing_files.append(source_object_key)
                logging.info(f'File: {key_to_check} was not copied.')

    if len(missing_files) == 0:
        logging.info("All files copied.")
    else:
        logging.info("The following files were not copied.")
        for missing_file in missing_files:
            logging.info(missing_file)


def copy_data_to_redshift():
    aws_hook = AwsHook(aws_conn_id='aws_credentials')
    credentials = aws_hook.get_credentials()
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    table_name = 'trips'
    s3_file_location = 's3://bikeshare-data-copy/divvy/unpartitioned/divvy_trips_2018.csv'
    redshift_hook = PostgresHook('redshift_connection')
    redshift_hook.run(copy_all_trips_sql.format(table_name, s3_file_location, access_key, secret_key))

copy_dag = DAG(
    'Copy_data_between_s3buckets',
    start_date=datetime.datetime.now()
)


s3_s3_copy_task = PythonOperator(
    task_id='Copy_s3_to_s3.task',
    python_callable=move_data_to_archive_bucket,
    dag=copy_dag
)

# validate_task = PythonOperator(
#     task_id='Check_copied_files.task',
#     python_callable=validate_s3_t0_s3_copy,
#     dag=copy_dag
# )
#
# create_trips_table = PostgresOperator(
#     task_id='Create_trips_table.task',
#     postgres_conn_id='redshift_connection',
#     sql=create_trips_table_sql
# )
#
# copy_trips_data = PythonOperator(
#     task_id='Copy_trips_data.task',
#     python_callable=copy_data_to_redshift,
#     dag=copy_dag
# )
#
# s3_s3_copy_task >> validate_task
# validate_task >> create_trips_table
# create_trips_table >> copy_trips_data