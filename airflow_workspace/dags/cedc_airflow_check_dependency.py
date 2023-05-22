from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow import DAG
import json

# 这些参数将传递给每个操作符
default_args = {
    'owner': 'liuzhu',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 21),
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

dag_name = 'cedc_airflow_test_check_dependency'

# 每个操作符的参数
job_parms_dict = {
    "dag_id": "dag_cedc_sales_pub",
    "waiting_time": 40,
    "max_waiting_count": 2,
    "base_url": "http://43.143.250.12:8080"
}
job_parms = json.dumps(job_parms_dict)

# timedelta 1: dag run by days
dag = DAG(
    dag_id=dag_name,
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
    bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger dependency_check --params \'{job_parms}\'',
    dag=dag
)

stop = BashOperator(
    task_id='stop',
    bash_command='echo stop',
    dag=dag
)

start >> dependency_check >> stop
