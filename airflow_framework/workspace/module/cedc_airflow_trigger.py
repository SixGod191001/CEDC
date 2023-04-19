from datetime import datetime,timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow import DAG

# These args will get passed on to each operator
default_args = {
    'owner': 'cedc_airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,4,21),
    'email': ['julie.wang@cognizant.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

dag_name = "cedc_airflow_trigger"

# parameters for each operator
job_parms = '"{\\"datasource\\": \\"parameter1_value\\", \\"parameter2\\": \\"parameter2_value\\"}"'

# timedelta 1: dag run by days
dag = DAG(
    dag_name,
    default_args=default_args,
    description='This DAG will run cedc_airflow_trigger, hard code parameters',
    schedule_interval=timedelta(days=1),
    catchup=False
)

start = BashOperator(
    task_id='start',
    bash_command='echo start',
    dag=dag
)

dependency_check = BashOperator(
    task_id='dependency_check',
    bash_command='python ' + Variable.get('main') + ' dependency_check ' + '' + '',
    dag=dag
)

kick_off = BashOperator(
    task_id=dag_name+'_job_kick_off_wrapper',
    bash_command='python '+Variable.get('main')+' start '+'name'+'' ,
    dag=dag
)

monitor = BashOperator(
    task_id=dag_name+'_job_monitoring_wrapper',
    bash_command='python '+Variable.get('main')+' monitor '+job_parms+'' ,
    dag=dag,
)

notify = BashOperator(
    task_id=dag_name+'_job_notify_wrapper',
    bash_command='python '+Variable.get('main')+' notify '+job_parms+'' ,
    dag=dag
)

trigger_next_dag = BashOperator(
    task_id=dag_name+'_job_trigger_next_dag_wrapper',
    bash_command='python '+Variable.get('main')+' trigger_next_dag '+job_parms+'' ,
    dag=dag
)

stop = BashOperator(
    task_id='stop',
    bash_command='echo stop',
    dag=dag
)

start >> dependency_check >> kick_off >> monitor >> notify >>trigger_next_dag >> stop
