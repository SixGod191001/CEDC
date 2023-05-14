import requests
import json
from airflow_workspace.utils.postgre_handler import PostgresHandler


# http://54.65.139.140:8080

class Trigger:
    # param dag_id: 通过main获取dag_id

    def __init__(self):
        self.url = 'http://54.65.139.140:8080'
        self.dag_id = ''

    def trigger_next_dag(self, event):

        self.url = event['url']
        self.dag_id = event['dag_id']

        pg_handler = PostgresHandler()

        # param dag_run_id: 根据main传入的'dag_id'在数据库中查找对应的dag_run_id
        sql_get_dag = f"select run_id from fact_dag_details WHERE dag_id = event['dag_id']"
        run_id = pg_handler.get_record(sql_get_dag)
        dag_run_id = run_id[0]['run_id']

        header = {'Authorization': 'Basic YWlyZmxvdzphaXJmbG93',
                  'Content-Type': 'application/json'}
        body = {
            "dag_run_id": dag_run_id
        }

        result = requests.get(
            f"{self.url}/api/v1/dags/{self.dag_id}/details",
            headers=header
        )

        # 判断airflow中dag是否存在，存在即调用，不存在返回错误信息
        try:
            dag_result = result.json()['dag_id']
            if not dag_result:
                raise KeyError("error")
            else:
                post = requests.post(
                    f"{self.url}/api/v1/dags/{self.dag_id}/dagRuns",
                    data=json.dumps(body),
                    headers=header)
                print(post.text)
        except KeyError as e:
            print("引发异常：", repr(e))

        return result


# if __name__ == "__main__":
#     event = {"dag_id": "dataset_consumes_1",
#              "url": "http://54.65.139.140:8080"
#              }
#     Trigger().trigger_next_dag(event)
