from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime, logging

def say_hello():
    logging.info("Hello the dag works")


dag = DAG(
    'first_dag',
    start_date=datetime.datetime.now()
)

operator_task = PythonOperator(
    task_id='first_running_task',
    dag=dag,
    python_callable=say_hello
)