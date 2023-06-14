from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow import DAG

# These args will get passed on to each operator
default_args = {
    'owner': 'cedc_airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 21),
    'email': ['julie.wang@cognizant.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

# timedelta 1: dag run by days
dag = DAG(
    dag_id='b',
    default_args=default_args,
    description='demo dag b',
    schedule_interval=timedelta(days=1),
    catchup=False
)

start = BashOperator(
    task_id='start',
    bash_command='echo start',
    dag=dag
)

stop = BashOperator(
    task_id='stop',
    bash_command='echo stop',
    dag=dag
)

start >> stop
