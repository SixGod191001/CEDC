# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/5/4 20:58
"""
import os
import logger_handler
from airflow.exceptions import AirflowFailException  # make the task failed without retry
from airflow.exceptions import AirflowException  # failed with retry

logger = logger_handler.logger()


def get_airflow_task_info():
    """
    从当前环境变量获取当前task以及dag的信息 如下例子：
    AIRFLOW_CTX_DAG_OWNER=***
    AIRFLOW_CTX_DAG_ID=dag_id0000
    AIRFLOW_CTX_TASK_ID=task_id2222
    AIRFLOW_CTX_EXECUTION_DATE=2023-05-04T13:33:00.670150+00:00
    AIRFLOW_CTX_TRY_NUMBER=1
    AIRFLOW_CTX_DAG_RUN_ID=manual__2023-05-04T13:33:00.670150+00:00
    :return: os_env_var_json : dict
    """
    try:
        os_env_var_json = {"AIRFLOW_CTX_DAG_OWNER": os.environ["AIRFLOW_CTX_DAG_OWNER"],
                           "AIRFLOW_CTX_DAG_ID": os.environ["AIRFLOW_CTX_DAG_ID"],
                           "AIRFLOW_CTX_TASK_ID": os.environ["AIRFLOW_CTX_TASK_ID"],
                           "AIRFLOW_CTX_TRY_NUMBER": os.environ["AIRFLOW_CTX_TRY_NUMBER"],
                           "AIRFLOW_CTX_EXECUTION_DATE": os.environ["AIRFLOW_CTX_EXECUTION_DATE"],
                           "AIRFLOW_CTX_DAG_RUN_ID": os.environ["AIRFLOW_CTX_DAG_RUN_ID"]
                           }
        logger.info("AIRFLOW_CTX_DAG_OWNER is: {}".format(os_env_var_json['AIRFLOW_CTX_DAG_OWNER']))
        logger.info("AIRFLOW_CTX_DAG_ID is: {}".format(os_env_var_json['AIRFLOW_CTX_DAG_ID']))
        logger.info("AIRFLOW_CTX_TASK_ID is: {}".format(os_env_var_json['AIRFLOW_CTX_TASK_ID']))
        logger.info("AIRFLOW_CTX_TRY_NUMBER is: {}".format(os_env_var_json['AIRFLOW_CTX_TRY_NUMBER']))
        logger.info("AIRFLOW_CTX_EXECUTION_DATE is: {}".format(os_env_var_json['AIRFLOW_CTX_EXECUTION_DATE']))
        logger.info("AIRFLOW_CTX_DAG_RUN_ID is: {}".format(os_env_var_json['AIRFLOW_CTX_DAG_RUN_ID']))
        return os_env_var_json
    except Exception as e:
        raise AirflowException("Couldn't get AIRFLOW_CTX variables.")


if __name__ == "__main__":
    get_airflow_task_info()
