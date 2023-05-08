# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/5/8 20:00
"""
from datetime import datetime, timedelta, time
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from airflow import DAG
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

dag_list = [{'dag_name': 'a', 'flag': 'success'},
            {'dag_name': 'b', 'flag': 'failed'},
            {'dag_name': 'c', 'flag': 'success'},
            {'dag_name': 'd', 'flag': 'success'},
            {'dag_name': 'e', 'flag': None},
            {'dag_name': 'f', 'flag': 'success'},

            ]


def process(dag_name, flag, **context):
    """
    :param dag_name: dag id
    :param flag: dag再数据库里执行成功还是失败
    :return:
    """
    run_id = os.environ["AIRFLOW_CTX_DAG_RUN_ID"]
    is_manual = run_id.startswith('manual__')
    if is_manual:
        TriggerDagRunOperator(
            task_id='start',
            trigger_dag_id=dag_name
        ).execute(context)
    else:
        if len(flag) == 0:
            raise AirflowSkipException
        elif flag == 'success':
            pass
        elif flag == 'failed':
            raise AirflowFailException


with DAG(
        dag_id="dag_diagram",
        schedule_interval='5 * * * 1-5',
        catchup=False,
        tags=["sales"],
        default_args=default_args
) as dag:
    names = locals()
    for i, element in enumerate(dag_list):
        names['%s' % element['dag_name']] = PythonOperator(
            task_id=f"{element['dag_name']}",
            python_callable=process,
            op_kwargs={"dag_name": element['dag_name'], "flag": element['flag']},
            dag=dag
        )

    chain(a, [b, c], [d, e], f)

