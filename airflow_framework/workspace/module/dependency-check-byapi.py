import requests
import time
class MyDependencyChecker:
    def __init__(self, task_id, dag_id, dag_run_id, waiting_time=60):
        self.task_id = task_id
        self.dag_id = dag_id
        self.dag_run_id = dag_run_id
        self.waiting_time = waiting_time

    def get_task_status(self):
        # http://43.143.250.12:8080/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances
        # https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html#operation/get_task_instances
        # https://airflow.apache.org/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}
        job_status_api_url = "http://43.143.250.12:8080/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
        job_status_api_url = job_status_api_url.format(dag_id=self.dag_id, dag_run_id=self.dag_run_id, task_id=self.task_id)
        # 请求中加入Authorization
        response = requests.get(job_status_api_url, headers={'Authorization':'Basic YWlyZmxvdzphaXJmbG93'})
        response.raise_for_status()
        print(response.json()['state'])
        return response.json()['state']

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

