from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow import DAG
import json

# These args will get passed on to each operator
default_args = {
    'owner': 'haojing',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 21),
    'email': ['jing.hao@cognizant.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

dag_name = 'cedc_airflow_test_next_dag'

# parameters for each operator
job_parms = {"datasource_name": "dag_cedc_sales_prelanding",
             "dag_run_id": "7889989765",
             "load_type": "ALL",
             "run_type": "glue",
             "glue_template_name": "devops.prelanding.s3_file_movement",
             "dag_id": "first_dag",
             "execution_date": f"{datetime.now().date()}",
             "waiting_time": 4,
             "max_waiting_count": 2,
             "base_url": "http://43.143.250.12:8080",
             "status": "Succeed",
             "job_name": "demo"}
job_parms = json.dumps(job_parms)

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
#
# dependency_check = BashOperator(
#     task_id='dependency_check',
#     bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger dependency_check --params \'{job_parms}\'',
#     dag=dag
# )
#
# kick_off = BashOperator(
#     task_id=dag_name + '_job_kick_off_wrapper',
#     bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger start_batch --params \'{job_parms}\'',
#     dag=dag
# )
#
# monitor = BashOperator(
#     task_id=dag_name + '_job_monitoring_wrapper',
#     bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger monitor_batch --params \'{job_parms}\'',
#     dag=dag,
# )
#
# notify = BashOperator(
#     task_id=dag_name + '_job_notify_wrapper',
#     bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger batch_notify --params \'{job_parms}\'',
#     dag=dag
# )

trigger_next_dag = BashOperator(
    task_id=dag_name + '_job_trigger_next_dag_wrapper',
    bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger trigger_next_dag --params \'{job_parms}\'',
    dag=dag
)

stop = BashOperator(
    task_id='stop',
    bash_command='echo stop',
    dag=dag
)

start >> trigger_next_dag >> stop
