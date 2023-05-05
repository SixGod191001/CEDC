# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/5/4 19:47
"""
from airflow.models import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

dag = DAG(
    dag_id='dag_id0000',
    schedule_interval=None,
    start_date=datetime(2023, 5, 3)
)

t1 = BashOperator(
    task_id='task_id1111',
    bash_command='echo {{ run_id }}',
    dag=dag
)

t2 = BashOperator(
    task_id='task_id2222',
    bash_command='date',
    dag=dag
)

t3 = BashOperator(
    task_id='task_id3333',
    bash_command=Variable.get('python') + ' /home/ubuntu/airflow_workspace/utils/get_airflow_task_info.py',
    dag=dag
)

t4 = ExternalTaskSensor(
    task_id="child_task1",
    external_dag_id='tutorial',
    external_task_id='print_date',
    timeout=600,
    allowed_states=["success"],
    failed_states=["failed", "skipped"],
    mode="reschedule",
    dag=dag
)
t1 >> t2 >> t3 >> t4
