from airflow_workspace.module.dag import process_dag
from airflow_workspace.module.dependency import Dependency
from airflow_workspace.module.monitor import Monitor
from airflow_workspace.module.notify import Notify
from airflow_workspace.module.start import Start
from airflow_workspace.module.trigger import Trigger
"""mv 
    公用方法
    存放不同模块调用所对应的方法
    """

def start_batch(event):
    try:
        Start().run(event)
        return "Successfully run the batch"
    except Exception as e:
        # insrt log here
        exit(1)


def monitor_batch(event):
    try:
        Monitor().monitor(event)
        return "Completed monitoring"
    except Exception as e:
        # insrt log here
        exit(1)


def batch_notify(event):
    try:
        Notify().send_job_result(event)
        return "Successfully send notification"
    except Exception as e:
        # insrt log here
        exit(1)


def dependency_check(event):
    try:
        Dependency().check_dependencies(event)
        return "Completed dependency checking"
    except Exception as e:
        # insrt log here
        exit(1)


def trigger_next_dag(event):
    try:
        Trigger().trigger_next_dag(event)
        return "Successfully trigger next dag"
    except Exception as e:
        # insrt log here
        exit(1)

def start_dag(event):
    try:
        process_dag().start_dag(event)
        return "Successfully run the batch"
    except Exception as e:
        # insrt log here
        exit(1)