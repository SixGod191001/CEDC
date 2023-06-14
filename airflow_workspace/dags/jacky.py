from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow import DAG
import json

# These args will get passed on to each operator
default_args = {
    'owner': 'luyan',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 21),
    'email': ['luyanlovely@163.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

dag_name = 'jacky'

# parameters for each operator
job_parms_dict = {"datasource_name": "sample",
                  "dag_run_id": "78899",
                  "load_type": "ALL",
                  "run_type": "glue",
                  "glue_template_name": "devops.prelanding.s3_file_movement",
                  "dag_id": "dag_cedc_sales_pub",
                  "waiting_time": 40,
                  "max_waiting_count": 2,
                  "base_url": "http://43.143.250.12:8080",
                  "status": "Succeed",
                  "job_name": "demo"}
json_str = json.dumps(job_parms_dict).replace('"', '\\"')

job_parms = '"' + f"{json_str}" + '"'
print("job_parms is {}".format(job_parms))
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
dependency_check = BashOperator(
    task_id='dependency_check',
    bash_command=Variable.get('python') + ' ' + Variable.get(
        'main') + ' --trigger=' + 'dependency_check' + ' --params=' + "'"+ job_parms + "'",
    # bash_command = "python /home/ubuntu/dags/cedc_airflow_test.py "+ 'dependency_check' + job_parms,
    dag=dag
)
#
# kick_off = BashOperator(
#     task_id=dag_name + '_job_kick_off_wrapper',
#     bash_command=Variable.get('python') + ' ' + Variable.get(
#         'main') + ' --trigger=' + 'start_batch' + ' --params=' + job_parms + '',
#     dag=dag
# )
#
# monitor = BashOperator(
#     task_id=dag_name + '_job_monitoring_wrapper',
#     bash_command=Variable.get('python') + ' ' + Variable.get(
#         'main') + ' --trigger=' + 'monitor_batch' + ' --params=' + job_parms + '',
#     dag=dag,
# )
#
# notify = BashOperator(
#     task_id=dag_name + '_job_notify_wrapper',
#     bash_command=Variable.get('python') + ' ' + Variable.get(
#         'main') + ' --trigger=' + 'batch_notify' + ' --params=' + job_parms + '',
#     dag=dag
# )
#
# trigger_next_dag = BashOperator(
#     task_id=dag_name + '_job_trigger_next_dag_wrapper',
#     bash_command=Variable.get('python') + ' ' + Variable.get(
#         'main') + ' --trigger=' + 'trigger_next_dag' + ' --params=' + job_parms + '',
#     dag=dag
# )

stop = BashOperator(
    task_id='stop',
    bash_command='echo stop',
    dag=dag
)

start >> dependency_check >> stop
