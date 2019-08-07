from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os, datetime, logging

summary_today_dag = DAG(
    'Environment_Summary',
    start_date=datetime.datetime.now() - datetime.timedelta(days=2),
    schedule_interval='@daily'
)


def say_hello():
    logging.info('Hello. Welcome.')


def log_time():
    logging.info(f'Current time is {datetime.datetime.utcnow().isoformat()}')


def log_directory():
    logging.info(f'Currently working directory: {os.getcwd()}')


def log_ending():
    logging.info('Logging has finished successfully in order.')


hello_task = PythonOperator(
    task_id='say_hello.task',
    python_callable=say_hello,
    dag=summary_today_dag
)

time_task = PythonOperator(
    task_id='log_time.task',
    python_callable=log_time,
    dag=summary_today_dag
)

dir_task = PythonOperator(
    task_id='log_dir.task',
    python_callable=log_directory,
    dag=summary_today_dag
)

end_task = PythonOperator(
    task_id='log_end.task',
    python_callable=log_ending,
    dag=summary_today_dag
)

"""
             time_task
hello_task _/        \_ end_task
            \        /
             dir_task
"""

hello_task >> time_task
hello_task >> dir_task
time_task >> end_task
dir_task >> end_task
