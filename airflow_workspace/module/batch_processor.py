from airflow_workspace.module.dag import process_dag
from airflow_workspace.module.dependency import Dependency
from airflow_workspace.module.monitor import Monitor
from airflow_workspace.module.notify import Notify
from airflow_workspace.module.start import Start
from airflow_workspace.module.trigger import Trigger
from airflow_workspace.utils.exception_handler import catch_fail_exception

"""mv 
    公用方法
    存放不同模块调用所对应的方法
    """
@catch_fail_exception
def start_batch(event):
    Start().run(event)

@catch_fail_exception
def monitor_batch(event):
    Monitor().monitor(event)

@catch_fail_exception
def batch_notify(event):
    Notify().send_job_result(event)

@catch_fail_exception
def dependency_check(event):
    Dependency().check_dependencies(event)

@catch_fail_exception
def trigger_next_dag(event):
    Trigger().trigger_next_dag(event)
@catch_fail_exception
def start_dag(event):
    process_dag().start_dag(event)
@catch_fail_exception
def stop_dag(event):
    process_dag().dag_check(event)
