from airflow_workspace.module.dependency import Dependency
from airflow_workspace.module.monitor import Monitor
from airflow_workspace.module.notify import Notify
from airflow_workspace.module.start import Start
from airflow_workspace.module.trigger import Trigger
from airflow_workspace.utils.exception_handler import catch_fail_exception
from airflow_workspace.utils.logger_handler import logger
"""
    公用方法
    存放不同模块调用所对应的方法
    """
logger = logger()
@catch_fail_exception
def start_batch(event):
    Start().run(event)
    logger.info("Successfully run the batch")

@catch_fail_exception
def monitor_batch(event):
    Monitor().monitor(event)
    logger.info("Completed monitoring")


@catch_fail_exception
def batch_notify(event):
    Notify().send_job_result(event)
    logger.info("Successfully send notification")


@catch_fail_exception
def dependency_check(event):
    Dependency().check_dependencies(event)
    logger.info("Completed dependency checking")


@catch_fail_exception
def trigger_next_dag(event):
    Trigger().trigger_next_dag(event)
    logger.info("Successfully trigger next dag")

