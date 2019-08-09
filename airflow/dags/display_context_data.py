from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging, datetime


def log_tomorrow_date(*args, **kwargs):
    logging.info(f"Tomorrow's date is {kwargs['tomorrow_ds']}")


tomorrow_date_from_context_dag = DAG(
    'Tomorrow_date_from_context',
    start_date=datetime.datetime.now() - datetime.timedelta(days=2),
    schedule_interval='@daily'
)



log_tomorrow_date_task = PythonOperator(
    task_id='log_tomorrow_date.task',
    python_callable=log_tomorrow_date,
    dag=tomorrow_date_from_context_dag,
    provide_context=True
)