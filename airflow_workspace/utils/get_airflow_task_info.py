# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/5/4 20:58
"""
import os
from airflow.sensors.external_task import ExternalTaskSensor
import logging

logger = logging.getLogger(__name__)


def get_airflow_task_info():
    """
    获取当前task以及dag的信息 如下例子：
    AIRFLOW_CTX_DAG_OWNER=***
    AIRFLOW_CTX_DAG_ID=dag_id0000
    AIRFLOW_CTX_TASK_ID=task_id2222
    AIRFLOW_CTX_EXECUTION_DATE=2023-05-04T13:33:00.670150+00:00
    AIRFLOW_CTX_TRY_NUMBER=1
    AIRFLOW_CTX_DAG_RUN_ID=manual__2023-05-04T13:33:00.670150+00:00
    :return: os_env_var_json : dict
    """
    logger.info("This is a log message")
    os_env_var_json = {"AIRFLOW_CTX_DAG_OWNER": os.environ["AIRFLOW_CTX_DAG_OWNER"],
                       "AIRFLOW_CTX_DAG_ID": os.environ["AIRFLOW_CTX_DAG_ID"],
                       "AIRFLOW_CTX_TASK_ID": os.environ["AIRFLOW_CTX_TASK_ID"],
                       "AIRFLOW_CTX_TRY_NUMBER": os.environ["AIRFLOW_CTX_TRY_NUMBER"],
                       "AIRFLOW_CTX_EXECUTION_DATE": os.environ["AIRFLOW_CTX_EXECUTION_DATE"],
                       "AIRFLOW_CTX_DAG_RUN_ID": os.environ["AIRFLOW_CTX_DAG_RUN_ID"]
                       }
    return os_env_var_json


child_task1 = ExternalTaskSensor(
    task_id="child_task1",
    external_dag_id='tutorial',
    external_task_id='print_date',
    timeout=600,
    allowed_states=["success"],
    failed_states=["failed", "skipped"],
    mode="reschedule",
)

if __name__ == "__main__":
    get_airflow_task_info()
