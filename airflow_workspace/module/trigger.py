import requests
import json
from airflow_workspace.utils.hash_handler import gen_random_hasher
from airflow_workspace.utils.postgre_handler import PostgresHandler


class Trigger:
    # 调用下一个Dag
    # dag_run_id: 通过随机数获取

    def __init__(self):
        self.dag_id = ''
        self.url = 'http://18.183.162.78'

    def trigger_next_dag(self, event):
        self.dag_id = event['dag_id']
        self.url = event['url']

        run_id = gen_random_hasher()

        pg_handler = PostgresHandler()

        # param dag_id: 根据main传入的'dag_id'在数据库中查找是否存在
        sql_get_dag = f"select dag_name from dim_dag WHERE dag_name = {self.dag_id} and is_active = 'Y'"
        get_dag = pg_handler.get_record(sql_get_dag)

        if not get_dag:
            print("引发异常：dag_name 为空或不存在")
        else:
            header = {'Authorization': 'Basic YWlyZmxvdzphaXJmbG93',
                      'Content-Type': 'application/json'}

            body = {
                "dag_run_id": run_id
            }

            post = requests.post(
                f"{self.url}/api/v1/dags/{self.dag_id}/dagRuns",
                data=json.dumps(body),
                headers=header)
            print(post.text)


# if __name__ == "__main__":
#     event = {"dag_id": "dataset_consumes_145678889",
#              "url": "http://18.183.162.78"
#              }
#     Trigger().trigger_next_dag(event)
