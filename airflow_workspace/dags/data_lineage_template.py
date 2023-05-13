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
from collections import Counter

# from airflow_workspace.utils import postgre_handler

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
# dag_list = postgre_handler.PostgresHandler.get_record("select * from xxx")
dag_list = [{'dag_name': 'a', 'flag': 'success', 'tag': 'department1,dalian,sales'},
            {'dag_name': 'b', 'flag': 'failed', 'tag': 'department2,beijing,sales'},
            {'dag_name': 'c', 'flag': 'running', 'tag': 'department1,dalian,sales'},
            {'dag_name': 'd', 'flag': 'success', 'tag': 'department1,dalian,sales'},
            {'dag_name': 'e', 'flag': 'success', 'tag': 'department2,beijing,sales'},
            {'dag_name': 'f', 'flag': 'success', 'tag': 'department2,beijing,sales'},
            {'dag_name': 'g', 'flag': 'success', 'tag': 'department2,beijing,sales'},
            {'dag_name': 'h', 'flag': 'success', 'tag': 'department3,shanghai,sales'},
            {'dag_name': 'w', 'flag': 'success', 'tag': 'department3,shanghai,sales'},
            ]
tag_list = sorted(list(set([str(tag) for dag in dag_list for tag in dag['tag'].split(',')])))
# 取父子关系
# lst = postgre_handler.PostgresHandler.get_record("select * from xxx")
lst = [["a", "b"], ["a", "c"], ["b", "d"], ["c", "e"], ["d", "f"], ["e", "f"], ["g", "h"], [None, "w"]]


def process(dag_name, flag, **context):
    is_manual = os.environ["AIRFLOW_CTX_DAG_RUN_ID"].startswith('manual__')
    ti = context['ti']
    if is_manual:
        TriggerDagRunOperator(task_id='start', trigger_dag_id=dag_name).execute(context)
    else:
        if len(flag) == 0:
            ti.set_state(State.NONE)
            raise AirflowSkipException
        elif flag == 'failed':
            raise AirflowFailException
        elif flag == 'running':
            ti.set_state(State.RUNNING)
            raise AirflowSkipException
        else:
            pass


with DAG(
        dag_id="data_lineage1",
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

    for idx, tag in enumerate(tag_list):
        _['%s' % tag] = BashOperator(task_id=f"{tag}", bash_command=f'echo {tag}', dag=dag)

    head, tail = list(set([x[0] for x in lst if x[0] is not None]) - set([y[1] for y in lst])), list(
        set([x[1] for x in lst]) - set([y[0] for y in lst if y[0] is not None]))

    for tag in tag_list: start >> _.get(tag)

    for i in dag_list:
        if i['dag_name'] in head or (i['dag_name'] in tail and i['dag_name'] in [node[1] for node in lst if node[0] is None]):
            for t in str(i['tag']).split(','):
                _.get(str(t)) >> _.get(i['dag_name'])

    for j in tail: _.get(j) >> stop

    for k in lst: _.get(k[1]) >> stop if k[0] is None else _.get(k[0]) >> _.get(k[1])
