from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow import DAG
import json

# 这些参数将传递给每个操作符
default_args = {
    'owner': 'lin',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 11),
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

dag_name = 'lin'

# 每个操作符的参数
job_parms_dict = {
    "dag_name": "dag_cedc_sales_prelanding",
    "task_name": "task_cedc_sales_prelanding_push_params"
    # "base_url": '{Variable.get("base_url")}'
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

insert_dag = BashOperator(
    task_id='insert_dag',
    bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger insert_dag --params \'{job_parms}\'',
    dag=dag
)


start_batch = BashOperator(
    task_id=dag_name + '_job_kick_off_wrapper',
    bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger start_batch --params \'{job_parms}\'',
    dag=dag
)


stop = BashOperator(
    task_id='stop',
    bash_command='echo stop',
    dag=dag
)


#
monitor = BashOperator(
    task_id=dag_name + '_job_monitoring_wrapper',
    bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger monitor_batch --params \'{job_parms}\'',
    dag=dag,
)
start >> insert_dag >> start_batch >> monitor >> stop

#
# notify = BashOperator(
#     task_id=dag_name + '_job_notify_wrapper',
#     bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger batch_notify --params \'{job_parms}\'',
#     dag=dag
# )

# trigger_next_dag = BashOperator(
#     task_id=dag_name + '_job_trigger_next_dag_wrapper',
#     bash_command=f'{Variable.get("python")} {Variable.get("main")} --trigger trigger_next_dag --params \'{job_parms}\'',
#     dag=dag
# )
#
# stop = BashOperator(
#     task_id='stop',
#     bash_command='echo stop',
#     dag=dag
# )
#
# start >> monitor >> stop
