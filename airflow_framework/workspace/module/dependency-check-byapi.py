# -*- coding: utf-8 -*-
"""
@Author : Liu Zhu
@Date : 2023/4/21 11:31
"""
import requests
import time
import logging

logging.basicConfig(level=logging.INFO)

class MyDependencyChecker:
    """
    依赖任务检查器
    :param task_id: 任务ID
    :param dag_id: DAG ID
    :param dag_run_id: DAG运行ID
    :param waiting_time: 等待时间
    :return: True/False
    """
    def __init__(self, task_id, dag_id, dag_run_id, waiting_time=60, base_url="http://43.143.250.12:8080"):
        self.task_id = task_id
        self.dag_id = dag_id
        self.dag_run_id = dag_run_id
        self.waiting_time = waiting_time
        self.base_url = base_url

    def get_task_status(self):
        # https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html#operation/get_task_instances
        # https://airflow.apache.org/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}
        job_status_api_url = f"{self.base_url}/api/v1/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances/{self.task_id}"
        # 请求中加入Authorization
        response = requests.get(job_status_api_url, headers={'Authorization':'Basic YWlyZmxvdzphaXJmbG93'})

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logging.error(f"Error occurred while fetching task status. Response code: {response.status_code}. Error message: {e}")

        task_state = response.json()['state']
        logging.info(f"Task state for task id {self.task_id} is {task_state}")
        return task_state

    def check_dependencies(self):
        count = 0
        while True:
            status = self.get_task_status()
            if status == 'success':
                return True
            elif status in {'queued', 'running'}:
                count += 1
                if count >= 3:
                    raise ValueError('任务等待时间过长')
            else:
                return False
            time.sleep(self.waiting_time)

dc = MyDependencyChecker(task_id='training_model_B', dag_id='first_dag', dag_run_id='manual__2023-04-21T02:23:29.529097+00:00')
dc.check_dependencies()

