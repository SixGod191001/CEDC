# -*- coding: utf-8 -*-
"""
@Author : Liu Zhu
@Date : 2023/5/9 16:31
"""
import requests
from datetime import timedelta
from airflow.exceptions import AirflowFailException  # make the task failed without retry
from airflow.exceptions import AirflowException  # failed with retry
from airflow_workspace.utils.logger_handler import logger
from datetime import datetime
logger = logger()

class AirflowAPIHandler:
    def __init__(self, base_url):
        self.base_url = base_url
 

    def get_dag_status_by_api(self, dag_id):
        # 构造查询参数
        current_datetime = datetime.now()  # 默认查询当天的dag运行状态
        formatted_datetime = current_datetime.strftime("%Y-%m-%d")
        # 将格式化后的日期字符串转换为 datetime 对象
        converted_datetime = datetime.strptime(formatted_datetime, "%Y-%m-%d")
        execution_date = converted_datetime 
        start_date_str = execution_date.isoformat() + 'Z'
        end_date_str = (execution_date + timedelta(days=1) - timedelta(seconds=1)).isoformat() + 'Z'


        # 构造请求URL和参数
        dag_run_api_url = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns"
        params = {
            'execution_date_gte': start_date_str,
            'execution_date_lte': end_date_str,
            'order_by': '-execution_date',
            'limit': '1'
        }

        # 发起请求
        headers = {'Authorization': 'Basic YWlyZmxvdzphaXJmbG93'}
        response = requests.get(dag_run_api_url, params=params, headers=headers)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise AirflowFailException(f"Error occurred while fetching DAG status. Response code: {response.status_code}. Error message: {e}")

        dag_runs = response.json()['dag_runs']
        if not dag_runs:
            raise AirflowFailException(f'{execution_date}这天没有dag id为 {dag_id} 的dag运行记录')

        # 获取state
        last_dag_run = dag_runs[0]
        dag_state = last_dag_run['state']

        return dag_state
    
if __name__ == '__main__':

    # 创建AirflowAPIUtils实例
    api_utils = AirflowAPIHandler("http://43.143.250.12:8080")

    # 获取DAG状态
    dag_id = "first_dag"
    dag_state = api_utils.get_dag_status_by_api(dag_id)

    # 打印DAG状态
    print("DAG state:", dag_state)





