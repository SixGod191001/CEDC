import requests
import json

from airflow_workspace.utils.exception_handler import catch_exception
from airflow_workspace.utils.postgre_handler import PostgresHandler
import random


class Trigger:
    # 调用下一个Dag
    # dag_run_id: 通过随机数获取

    def __init__(self):
        self.dag_id = ''
        self.url = 'http://43.143.250.12:8080'

    @catch_exception
    def trigger_next_dag(self, event):
        self.dag_id = event['dag_id']
        self.url = event['base_url']
        print(self.dag_id)
        random.seed()
        run_id = random.randint(10000001, 19999999)

        pg_handler = PostgresHandler()

        # param dag_run_id: 根据main传入的'dag_id'在数据库中查找对应的dag_run_id
        sql_get_dags = f"""
        select distinct dag_name from dim_dag_dependence WHERE dependency_dag_name = '{self.dag_id}' and is_active = 'Y'  
        and dag_name <>'dag_cedc_stop'
                       f"""
        get_dags = pg_handler.get_record(sql_get_dags)

        for i in get_dags:
            values = list(i.values())
            dag_name = values[0]
            if not dag_name:
                print("引发异常：dag_name 为空或不存在")
            else:
                header = {'Authorization': 'Basic YWlyZmxvdzphaXJmbG93',
                          'Content-Type': 'application/json'}

                body = {
                    "dag_run_id": str(run_id)
                }
            post = requests.post(
                f"{self.url}/api/v1/dags/{dag_name}/dagRuns",
                data=json.dumps(body),
                headers=header)
            print(post.text)


if __name__ == "__main__":
    event = {"dag_id": "dag_cedc_sales_a",
             "base_url": "http://43.143.250.12:8080"
             }
    Trigger().trigger_next_dag(event)
