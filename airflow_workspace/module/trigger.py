import json
import requests

dag_id = "dataset_consumes_1"


class Trigger:
    # 调用下一个Dag
    # param dag_run_id: 获取dag_run_id

    def __init__(self):
        pass

    def trigger_dag(self, event):        
        body = {
            "dag_run_id": event['dag_run_id']
        }

        header = {'Authorization': 'Basic YWlyZmxvdzphaXJmbG93',
                  'Content-Type': 'application/json'}

        result = requests.post(
            f"http://35.78.175.197:8080/api/v1/dags/{dag_id}/dagRuns",
            data=json.dumps(body),
            headers=header
        )
#35.78.175.197 43.143.250.12
        print(result.text)
        return result

# if __name__ == "__main__":
#   event = {"dag_run_id": "hgjfgkflglg"}
#   Trigger.trigger_dag(event)
