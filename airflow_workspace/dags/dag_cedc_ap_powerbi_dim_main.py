from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow import DAG
import json
# These args will get passed on to each operator
default_args = {
    'owner': 'cedc',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 21),
    'email': ['yan.lu@cognizant.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

dag_name = 'dag_cedc_ap_powerbi_dim_main'

# parameters for each operator
job_parms = {"task_name": "task_cedc_ap_powerbi_dim_main",
                 "dag_id": "dag_cedc_ap_powerbi_dim_main",
                 "base_url": f'{Variable.get("base_url")}'
                 }
job_parms=json.dumps(job_parms)

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

start_dag = BashOperator(
    task_id='start_dag',
    bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger start_dag --params \'{job_parms}\'',
    dag=dag
)

dependency_check = BashOperator(
    task_id='dependency_check',
    bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger dependency_check --params \'{job_parms}\'',
    dag=dag
)

kick_off = BashOperator(
    task_id=dag_name + '_job_kick_off_wrapper',
    bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger start_batch --params \'{job_parms}\'',
    dag=dag
)

monitor = BashOperator(
    task_id=dag_name + '_job_monitoring_wrapper',
    bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger monitor_batch --params \'{job_parms}\'',
    dag=dag,
)
notify = BashOperator(
    task_id=dag_name + '_job_notify_wrapper',
    bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger batch_notify --params \'{job_parms}\'',
    dag=dag
)

trigger_next_dag = BashOperator(
    task_id=dag_name + '_job_trigger_next_dag_wrapper',
    bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger trigger_next_dag --params \'{job_parms}\'',
    dag=dag
)

check_dag = BashOperator(
    task_id='check_dag',
    bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger stop_dag --params \'{job_parms}\'',
    dag=dag
)

stop = BashOperator(
    task_id='stop',
    bash_command='echo stop',
    dag=dag
)

start >> dependency_check >> start_dag  >> kick_off>> monitor >> notify >> check_dag >> trigger_next_dag >> stop
