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
from airflow_workspace.utils.email_handler import EmailHandler

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
        # self.waiting_time = event['waiting_time']
        # self.max_waiting_count = event['max_waiting_count']
        self.base_url = event['base_url']

        dag_handler = AirflowDagHandler(self.base_url)
        dag_info = dag_handler.get_dag_info(self.dag_id)  # 通过dag_id获取dag_info

        self.waiting_time = dag_info[0]['waiting_time']
        self.max_waiting_count = dag_info[0]['max_waiting_count']

        self.dag_ids = dag_handler.get_dependencies_dag_ids_by_db(self.dag_id)  # 通过dag_id获取依赖的dag_ids列表


        if not self.dag_ids:
            logger.info(f"数据库中不存在{self.dag_id}的依赖关系，跳过依赖检查")
            return

        for dag_id in self.dag_ids:
            count = 0
            while True:
                state_byapi = dag_handler.get_dag_state_by_api(dag_id)
                logger.info(f"API查询{self.dag_id}依赖的{dag_id}的最新状态为{state_byapi}")

                state_bydb = dag_handler.get_dag_state_by_db(dag_id)
                search_dependency_dagname = state_bydb[0]['dag_name']
                search_dependency_dag_state = state_bydb[0]['status']
                
                logger.info(f"数据库查询{self.dag_id}的依赖{search_dependency_dagname}的最新状态为{search_dependency_dag_state}")

                if state_byapi != search_dependency_dag_state:
                    logger.info(f"{self.dag_id}依赖的{dag_id}的状态不一致：API 状态为 {state_byapi}，数据库状态为 {search_dependency_dag_state}")
                    subject = " DAG 状态检查不一致"
                    body_text = f"{self.dag_id}依赖的{dag_id}的状态不一致：API 状态为 {state_byapi}，数据库状态为 {search_dependency_dag_state}"
                    email_handler=EmailHandler()
                    email_handler.send_email_ses(subject, body_text)

                if search_dependency_dag_state == 'success':
                    logger.info(f'任务{dag_id}check成功,为 {search_dependency_dag_state}')
                    break
                elif search_dependency_dag_state == 'failed':
                    logger.info(f'任务{dag_id}check失败，状态为 {search_dependency_dag_state}')
                    raise AirflowFailException(f'任务{dag_id}check失败，状态为 {search_dependency_dag_state}')
                else:
                    count += 1
                    if count >= self.max_waiting_count:
                        logger.info(f'任务等待时间过长，已等待 {self.max_waiting_count * self.waiting_time} 秒')
                        raise AirflowFailException(f'任务等待时间过长，已等待 {self.max_waiting_count * self.waiting_time} 秒')
                    
                time.sleep(self.waiting_time)

if __name__ == '__main__':

    checker = Dependency()
    event = {"dag_id": "dag_cedc_sales_pub",
            # "execution_date": datetime(2023, 4, 26),
            # "waiting_time": 60,
            # "max_waiting_count": 2,
             "base_url" : "http://43.143.250.12:8080"
        }
    checker.check_dependencies(event)


