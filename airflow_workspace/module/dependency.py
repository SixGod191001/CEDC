# -*- coding: utf-8 -*-
"""
@Author : Liu Zhu
@Date : 2023/4/16 1:31
"""
import requests
import time
from datetime import datetime, timedelta
from airflow.exceptions import AirflowFailException  # make the task failed without retry
from airflow.exceptions import AirflowException  # failed with retry
from airflow_workspace.utils.logger_handler import logger
from airflow_workspace.utils.airflow_api_handler import AirflowAPIHandler

logger = logger()

class Dependency:
    """
    DAG运行状态检查器
    :param dag_id: DAG ID
    :param execution_date: DAG运行日期 eg:datetime(2023, 4, 22)
    :param base_url: Airflow Web Server URL
    :param waiting_time: 等待时间/秒 默认60秒
    :param max_waiting_count: 最大等待次数/次 默认3次
    :return: DAG运行状态
    """

    def __init__(self):

        self.dag_ids = ''
        # self.execution_date = ''
        self.waiting_time = 60
        self.max_waiting_count = 3
        self.base_url = 'http://43.143.250.12:8080'

    def check_dependencies(self,event):
        self.event = event
        
        self.dag_ids = event['dag_ids']
        # self.execution_date = event['execution_date']
        self.waiting_time = event['waiting_time']
        self.max_waiting_count = event['max_waiting_count']
        self.base_url = event['base_url']

        api_utils = AirflowAPIHandler(self.base_url)

        for dag_id in self.dag_ids:
            count = 0
            while True:
                status = api_utils.get_dag_status_by_api(dag_id)
                logger.info(f"Current DAG status for dag id {dag_id}: {status}")
                if status == 'success':
                    logger.info(f'任务{dag_id}check成功,状态为 {status}')
                    break
                elif status in {'queued', 'running'}:
                    count += 1
                    if count >= self.max_waiting_count:
                        logger.info(f'任务等待时间过长，已等待 {self.max_waiting_count * self.waiting_time} 秒')
                        raise AirflowFailException(f'任务等待时间过长，已等待 {self.max_waiting_count * self.waiting_time} 秒')
                else:
                    logger.info(f'任务{dag_id}check失败，状态为 {status}')
                    raise AirflowFailException(f'任务{dag_id}check失败，状态为 {status}')
                time.sleep(self.waiting_time)

if __name__ == '__main__':

    checker = Dependency()
    event = {"dag_ids": ["first_dag", "second_dag"],
            # "execution_date": datetime(2023, 4, 26),
             "waiting_time": 4,
             "max_waiting_count": 2,
             "base_url" : "http://43.143.250.12:8080"
        }
    checker.check_dependencies(event)

