# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/5/8 20:00
"""
from datetime import datetime, timedelta
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.utils.state import State
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
from collections import Counter

from airflow_workspace.module.data_lineage import *
from airflow_workspace.utils.postgre_handler import PostgresHandler


# These args will get passed on to each operator
default_args = {
    'owner': 'luyan',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 21),
    'email': ['julie.wang@cognizant.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 0
}
tag= "'sales'"

dag_list=get_dag_list(tag)
lst=get_dag_chain(tag)
# tag_list = sorted(list(set([str(tag) for dag in dag_list for tag in dag['tag'].split(',')])))
tag_list=tag.replace("'",'').split(",")
def process(dag_name, flag, **context):
    is_manual = os.environ["AIRFLOW_CTX_DAG_RUN_ID"].startswith('manual__')
    ti = context['ti']
    if is_manual:
        TriggerDagRunOperator(task_id='start', trigger_dag_id=dag_name).execute(context)
    if len(flag) == 0:
        ti.set_state(State.NONE)
        raise AirflowSkipException
    elif flag.lower() == 'failed':
        raise AirflowFailException
    elif flag.lower() == 'running':
        ti.set_state(State.RUNNING)
        raise AirflowSkipException
    else:
        pass


with DAG(
        dag_id="data_lineage_sales",
        # schedule='*/10 * * * 1-5',
        schedule=timedelta(days=1),
        catchup=False,
        tags=tag_list,
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

    for idx, tag in enumerate(tag_list):
        _['%s' % tag] = BashOperator(task_id=f"{tag}", bash_command=f'echo {tag}', dag=dag)

    # head, tail = list(set([x[0] for x in lst if x[0] is not None]) - set([y[-1] for y in lst])), list(
    #     set([x[-1] for x in lst]) - set([y[0] for y in lst if y[0] is not None]))
    head = [k[1] for k in lst if k[0] == 'dag_cedc_start']
    tail = [k[0] for k in lst if k[1] == 'dag_cedc_stop']
    for tag in tag_list: start >> _.get(tag)

    for i in dag_list:
        if i['dag_name'] in head or (i['dag_name'] in tail and i['dag_name'] in [node[1] for node in lst if node[0] is None]):
            for t in str(i['tag']).split(','):
                _.get(str(t)) >> _.get(i['dag_name'])

    for j in tail: _.get(j) >> stop
    for k in lst:
        if k[0] is None:
            _.get(k[1]) >> stop
        elif k[0] != 'dag_cedc_start' and k[1] != 'dag_cedc_stop':
            _.get(k[0]) >> _.get(k[1])

