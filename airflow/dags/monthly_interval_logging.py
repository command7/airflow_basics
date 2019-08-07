from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime, logging

monthly_logging_dag = DAG(
    'Log_hello_monthly',
    start_date=datetime.datetime.now() - datetime.timedelta(days=60),
    schedule_interval='@monthly'
)

def log_hello():
    logging.info('Monthly dag working')


monthly_logger = PythonOperator(
    task_id='logging_monthly_dag_functioning.task',
    python_callable=log_hello,
    dag=monthly_logging_dag
)