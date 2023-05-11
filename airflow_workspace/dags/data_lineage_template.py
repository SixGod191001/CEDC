# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/5/8 20:00
"""
from datetime import datetime
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.utils.state import State
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os

# These args will get passed on to each operator
default_args = {
    'owner': 'cedc_airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 21),
    'email': ['julie.wang@cognizant.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 0
}

# 取状态
dag_list = [{'dag_name': 'a', 'flag': 'success'},
            {'dag_name': 'b', 'flag': 'failed'},
            {'dag_name': 'c', 'flag': 'running'},
            {'dag_name': 'd', 'flag': 'success'},
            {'dag_name': 'e', 'flag': 'success'},
            {'dag_name': 'f', 'flag': 'success'},
            {'dag_name': 'g', 'flag': 'success'},
            {'dag_name': 'h', 'flag': 'success'},
            {'dag_name': 'w', 'flag': 'success'},

            ]
# 取父子关系
lst = [["a", "b"], ["a", "c"], ["b", "d"], ["c", "e"], ["d", "f"], ["e", "f"], ["g", "h"], ["w", None]]


def process(dag_name, flag, **context):
    is_manual = os.environ["AIRFLOW_CTX_DAG_RUN_ID"].startswith('manual__')
    ti = context['ti']
    if is_manual:
        TriggerDagRunOperator(task_id='start', trigger_dag_id=dag_name).execute(context)
    else:
        if len(flag) == 0:
            ti.set_state(State.NONE)
        elif flag == 'success':
            pass
        elif flag == 'failed':
            raise AirflowFailException
        elif flag == 'running':
            ti.set_state(State.RUNNING)
            raise AirflowSkipException
        else:
            pass


with DAG(
        dag_id="data_lineage_for_sales_team",
        schedule_interval='*/10 * * * 1-5',
        catchup=False,
        tags=["sales", "dalian"],
        default_args=default_args
) as dag:
    _ = locals()

    start = BashOperator(task_id='start', bash_command='echo start', dag=dag)

    stop = BashOperator(task_id='stop', bash_command='echo stop', dag=dag)

    for idx, item in enumerate(dag_list):
        _['%s' % item['dag_name']] = PythonOperator(
            task_id=f"{item['dag_name']}",
            python_callable=process,
            provide_context=True,
            op_kwargs={"dag_name": item['dag_name'], "flag": item['flag']},
            dag=dag
        )

    head, tail = list(set([x[0] for x in lst]) - set([y[1] for y in lst if y[1] is not None])), list(
        set([x[1] for x in lst if x[1] is not None]) - set([y[0] for y in lst]))

    for i in head: start >> _.get(i)

    for j in tail: _.get(j) >> stop

    for k in lst: _.get(k[0]) >> stop if k[1] is None else _.get(k[0]) >> _.get(k[1])
