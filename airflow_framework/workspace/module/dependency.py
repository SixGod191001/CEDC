# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/16 1:31
"""
import requests
import time
from datetime import datetime, timedelta
import logging

class Dependency:
    """
    DAG运行状态检查器
    :param dag_id: DAG ID
    :param execution_date: DAG运行日期 eg:datetime(2023, 4, 22)
    :param base_url: Airflow Web Server URL
    :param waiting_time: 等待时间 默认60秒
    :param max_waiting_count: 最大等待次数 默认3次
    :return: DAG运行状态
    """

    def __init__(self):

        self.dag_id = ''
        self.execution_date = ''
        self.waiting_time = 60
        self.max_waiting_count = 3
        self.base_url = 'http://43.143.250.12:8080'


    def get_dag_status(self):
        # https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html#operation/get_dag_run
        # https://airflow.apache.org/api/v1/dags/{dag_id}/dagRuns

        # 构造查询参数
        start_date_str = self.execution_date.isoformat() + 'Z'
        end_date_str = (self.execution_date + timedelta(days=1) - timedelta(seconds=1)).isoformat() + 'Z'

        # http://43.143.250.12:8080/api/v1/dags/first_dag/dagRuns?execution_date_gte=2023-04-23T00:00:00Z&execution_date_lte=2023-04-23T23:59:59Z&order_by=-execution_date&limit=1
        # 构造请求url
        dag_run_api_url = f"{self.base_url}/api/v1/dags/{self.dag_id}/dagRuns"
        params = {
            'execution_date_gte': start_date_str,
            'execution_date_lte': end_date_str,
            'order_by': '-execution_date',  # 按execution_date降序排列
            'limit': '1'                # 只取最后一次运行的dag
        }

        # 请求中加入Authorization
        logging.info(f"Querying {dag_run_api_url} with params {params}")
        response = requests.get(dag_run_api_url, params=params, headers={'Authorization': 'Basic YWlyZmxvdzphaXJmbG93'})

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise ValueError(f"Error occurred while fetching DAG status. Response code: {response.status_code}. Error message: {e}")

        dag_runs = response.json()['dag_runs']
        if not dag_runs:
            raise ValueError(f'{self.execution_date}这天没有dag id为 {self.dag_id} 的dag运行记录')

        # 获取到state
        last_dag_run = dag_runs[0]
        dag_state = last_dag_run['state']

        logging.info(f"DAG state for dag id {self.dag_id} is {dag_state}")
        return dag_state

    def check_dependencies(self,event):
        self.event = event
        self.dag_id = event['dag_id']
        self.execution_date = event['execution_date']
        self.waiting_time = event['waiting_time']
        self.max_waiting_count = event['max_waiting_count']
        self.base_url = event['base_url']

        count = 0
        while True:
            status = self.get_dag_status()
            logging.info(f"Current DAG status for dag id {self.dag_id}: {status}")
            if status == 'success':
                return status
            elif status in {'queued', 'running'}:
                count += 1
                if count >= self.max_waiting_count:
                    raise ValueError(f'任务等待时间过长，已等待 {self.max_waiting_count * self.waiting_time} 秒')
            else:
                raise ValueError(f'任务{self.dag_id}check失败，状态为 {status}')
            time.sleep(self.waiting_time)


# if __name__ == '__main__':
#
#     checker = Dependency()
#     event = {"dag_id": "first_dag",
#              "execution_date": datetime(2023, 4, 23),
#              "waiting_time": 4,
#              "max_waiting_count": 2,
#              "base_url" : "http://43.143.250.12:8080"
#         }
#     checker.check_dependencies(event)

