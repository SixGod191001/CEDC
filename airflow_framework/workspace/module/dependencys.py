# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/16 1:31
"""
import time
from typing import TYPE_CHECKING, Any, Callable, Collection, Iterable
import requests
from datetime import datetime, timedelta
import logging
from airflow.utils.state import State

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

    def __init__(self, 
                execution_date, # 代表一个外部任务的执行日期
                external_dag_id: str | None = None,  # 代表一个外部任务的任务 ID
                external_dag_ids: Collection[str] | None = None,  # 代表一个外部任务的任务 ID 列表,
                allowed_states: Iterable[str] | None = None,  # 代表一个外部任务的允许状态
                waitting_states: Iterable[str] | None = None,  # 代表一个外部任务的等待状态
                failed_states: Iterable[str] | None = None,  # 代表一个外部任务的失败状态
                waiting_time=60,  # 等待时间 默认60秒
                max_waiting_count=3, # 最大等待次数 默认3次
                base_url="http://43.143.250.12:8080", # Airflow Web Server URL
                **kwargs
                ):
        
        super().__init__(**kwargs) # 调用父类的构造函数
        self.allowed_states = list(allowed_states) if allowed_states else [State.SUCCESS] # 如果 allowed_states 为空，则设置为 [SUCCESS]
        self.waitting_states = list(waitting_states) if waitting_states else []    # 如果 waitting_states 为空，则设置为 []
        self.failed_states = list(failed_states) if failed_states else []   # 如果 failed_states 为空，则设置为 []

        total_states = set(self.allowed_states + self.waitting_states + self.failed_states)  # 计算总状态，将这个列表转换为一个集合，这样集合中的每个元素都是唯一的，没有重复项。

        #  检查两个值是否相等。如果它们不相等，说明在 allowed_states、waitting_states 和 failed_states 中提供了重复值，因为集合中的元素是唯一的。
        #  如果存在重复的任务状态，则引发异常。
        if len(total_states) != len(self.allowed_states) + len(self.waitting_states) + len(self.failed_states):
            raise ValueError(
                "Duplicate values provided across allowed_states, waitting_states and failed_states."
            )

        # 如果 external_dag_ids 参数不存在，则将其设置为 None，以便在后面使用。这是为了避免引用空列表，因为它可能会导致错误。
        if not external_dag_ids:
            external_dag_ids = None

        # 检查是否同时提供了 external_dag_id 和 external_dag_ids 参数。如果是，则引发 ValueError 异常，因为只能提供其中之一。
        if external_dag_id is not None and external_dag_ids is not None:
            raise ValueError(
                "Only one of `external_dag_id` or `external_dag_ids` can be provided."
            )
        
        # 如果 external_task_id 存在，则它将被用作任务 ID 列表中的唯一任务 ID。
        # 如果 external_task_ids 存在，则它将用作任务 ID 列表。
        if external_dag_id is not None:
            external_dag_ids = [external_dag_id]

        #检查是否传递了 external_dag_id 或 external_dag_ids 参数。
        # 如果是，则检查所有状态是否属于DAG状态，并引发 ValueError 异常，给出一个错误消息，说明在这种情况下应该使用哪些状态。
        if external_dag_id or external_dag_ids :
            if not total_states <= set(State.dag_states):
                raise ValueError(
                   "Valid values for `allowed_states`, `waitting_states` and `failed_states` "
                    "when `external_dag_id` or `external_dag_ids` "
                    f"is not `None`: {State.dag_states}"
                )
        

        self.external_dag_id = external_dag_id
        self.external_dag_ids = external_dag_ids
        self.allowed_states = allowed_states
        self.waitting_states = waitting_states
        self.failed_states = failed_states
        self.execution_date = execution_date
        self.waiting_time = waiting_time
        self.max_waiting_count = max_waiting_count
        self.base_url = base_url

    def get_dag_status(self, dag_id):

        start_date_str = self.execution_date.isoformat() + 'Z'
        end_date_str = (self.execution_date + timedelta(days=1) - timedelta(seconds=1)).isoformat() + 'Z'

        dag_run_api_url = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns"
        params = {
            'execution_date_gte': start_date_str,
            'execution_date_lte': end_date_str,
            'order_by': '-execution_date', 
            'limit': '1'
        }

        logging.info(f"Querying {dag_run_api_url} with params {params}")
        response = requests.get(dag_run_api_url, params=params, headers={'Authorization': 'Basic YWlyZmxvdzphaXJmbG93'})

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise ValueError(f"Error occurred while fetching DAG status. Response code: {response.status_code}. Error message: {e}")

        dag_runs = response.json()['dag_runs']
        if not dag_runs:
            raise ValueError(f'{self.execution_date}这天没有dag id为 {dag_id} 的dag运行记录')

        last_dag_run = dag_runs[0]
        dag_state = last_dag_run['state']

        logging.info(f"DAG state for dag id {dag_id} is {dag_state}")
        return dag_state

    def check_dependencies(self):
        if self.external_dag_id is not None:
            external_dag_ids = [self.external_dag_id]
        else:
            external_dag_ids = self.external_dag_ids or []

        if not external_dag_ids:
            raise ValueError('请提供至少一个外部Dag ID。')

        for dag_id in external_dag_ids:
            dag_status = self.get_dag_status(dag_id)
            logging.info(f"Current DAG status for dag id {dag_id}: {dag_status}")
            if dag_status in self.allowed_states:
                continue
            elif dag_status in self.waitting_states:
                count = 0
                while True:
                    if self.get_dag_status(dag_id) in self.allowed_states:
                        break
                    elif count >= self.max_waiting_count:
                        raise ValueError(f'Dag:{dag_id}等待时间过长，已等待 {self.max_waiting_count * self.waiting_time} 秒')
                    else:
                        logging.info(f"Task {dag_id} is still in {dag_status} status, waiting for {self.waiting_time} seconds.")
                        count += 1
                        time.sleep(self.waiting_time)
            elif dag_status in self.failed_states:
                raise ValueError(f'Dag:{dag_id}在{self.execution_date}这天状态为{dag_status}')
            else:
                raise ValueError(f'Dag:{dag_id}状态为{dag_status}，不在允许状态中')
        return True

# checker = Dependency(dag_id='first_dag', execution_date=datetime(2023, 4, 25), waiting_time=4, max_waiting_count=2)
# dag_state = checker.get_dag_status()
# print(dag_state)

if __name__ == '__main__':
    dag_ids = ["first_dag","second_dag"]
    allowed_states = ["success"]
    waiting_states = ["running","queued"]
    failed_states = ["failed"]

    checker = Dependency(
        external_dag_ids=dag_ids,
        allowed_states=allowed_states,
        waitting_states=waiting_states,
        failed_states=failed_states,
        execution_date=datetime(2023, 4, 23),
        waiting_time=4,
        max_waiting_count=2,
        base_url="http://43.143.250.12:8080",
    )
    checker.check_dependencies()