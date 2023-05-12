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
from airflow_workspace.utils.airflow_dag_handler import AirflowDagHandler
from airflow_workspace.utils.postgre_handler import PostgresHandler

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

        self.dag_id = event['dag_id']  # 解析出main传过来的dag_id
        # self.execution_date = event['execution_date']
        self.waiting_time = event['waiting_time']
        self.max_waiting_count = event['max_waiting_count']
        self.base_url = event['base_url']
        dag_handler = AirflowDagHandler(self.base_url)
        
        self.dag_ids = dag_handler.get_dependencies_dag_ids_by_db(self.dag_id)  # 通过dag_id获取依赖的dag_ids列表

        for dag_id in self.dag_ids:
            count = 0
            while True:
                state_byapi = dag_handler.get_dag_state_by_api(dag_id)
                # state_bydb = 
                logger.info(f"Current DAG state for dag id {dag_id}: {state_byapi}")
                if state_byapi == 'success':
                    logger.info(f'任务{dag_id}check成功,API的状态为 {state_byapi}')
                    break
                elif state_byapi in {'queued', 'running'}:
                    count += 1
                    if count >= self.max_waiting_count:
                        logger.info(f'任务等待时间过长，已等待 {self.max_waiting_count * self.waiting_time} 秒')
                        raise AirflowFailException(f'任务等待时间过长，已等待 {self.max_waiting_count * self.waiting_time} 秒')
                else:
                    logger.info(f'任务{dag_id}check失败，API的状态为 {state_byapi}')
                    raise AirflowFailException(f'任务{dag_id}check失败，API的状态为 {state_byapi}')
                time.sleep(self.waiting_time)

if __name__ == '__main__':

    checker = Dependency()
    event = {"dag_id": "dag_cedc_sales_pub3",
            # "execution_date": datetime(2023, 4, 26),
             "waiting_time": 4,
             "max_waiting_count": 2,
             "base_url" : "http://43.143.250.12:8080"
        }
    checker.check_dependencies(event)


