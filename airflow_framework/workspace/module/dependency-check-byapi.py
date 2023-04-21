import requests
import time


class MyDependencyChecker:
    def __init__(self, task_id, dag_id, execution_date, waiting_time=60):
        self.task_id = task_id
        self.dag_id = dag_id
        self.execution_date = execution_date
        self.waiting_time = waiting_time

    def get_task_status(self):
        # https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html#operation/get_task_instances
        # https://airflow.apache.org/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}
        job_status_api_url = "https://airflow.apache.org/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
        job_status_api_url = job_status_api_url.format(dag_id=self.dag_id, execution_date=self.execution_date.isoformat(), task_id=self.task_id)
        response = requests.get(job_status_api_url)
        response.raise_for_status()
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
