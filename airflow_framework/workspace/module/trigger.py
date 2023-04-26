import json
import requests

dag_id = "dataset_consumes_1"


class Trigger:
    # 调用下一个Dag
    # param dag_run_id: 获取dag_run_id

    def __init__(self, event):
        self.dag_run_id = event

    def trigger_dag(self):
        body = {
            "dag_run_id": self.dag_run_id
        }

        header = {'Authorization': 'Basic YWlyZmxvdzphaXJmbG93',
                  'Content-Type': 'application/json'}

        result = requests.post(
            f"http://43.143.250.12:8080/api/v1/dags/{dag_id}/dagRuns",
            data=json.dumps(body),
            headers=header
        )

        print(result.text)
        return result


if __name__ == "__main__":
    a = Trigger('dag_run_id')
    a.trigger_dag()
