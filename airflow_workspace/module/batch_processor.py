import traceback

from airflow_workspace.module.dag import process_dag
from airflow_workspace.module.dependency import Dependency
from airflow_workspace.module.monitor import Monitor
from airflow_workspace.module.notify import Notify
from airflow_workspace.module.start import Start
from airflow_workspace.module.trigger import Trigger
from airflow_workspace.utils.exception_handler import catch_fail_exception
from airflow_workspace.utils.logger_handler import get_logger

logger = get_logger()


@catch_fail_exception
def start_batch(event):
    """
    Start a batch job using the Start module.

    This function initializes and runs a batch job with the provided event data.
    The `Start` class handles the execution of the batch job.

    Args:
        event (dict): Dictionary containing event data required to start the batch job.
    """
    try:
        logger.info("Starting batch with details: {}".format(str(event)))
        Start(event).run()
    except Exception as e:
        logger.error(e)
        traceback.print_exc()
        logger.error("Failed to start the batch {}".format(e))




@catch_fail_exception
def monitor_batch(event):
    """
    Monitor a batch job using the Monitor module.

    This function initializes and monitors a batch job with the provided event data.
    The `Monitor` class handles the monitoring of the batch job.

    Args:
        event (dict): Dictionary containing event data required to monitor the batch job.
    """
    Monitor().monitor(event)


@catch_fail_exception
def batch_notify(event):
    """
    Send job results notification using the Notify module.

    This function initializes and sends a notification with the job results
    using the provided event data. The `Notify` class handles the notification process.

    Args:
        event (dict): Dictionary containing event data required to send the job results notification.
    """
    Notify().send_job_result(event)


@catch_fail_exception
def dependency_check(event):
    """
    Check job dependencies using the Dependency module.

    This function initializes and checks the dependencies of a job using the provided
    event data. The `Dependency` class handles the dependency checks.

    Args:
        event (dict): Dictionary containing event data required to check the job dependencies.
    """
    Dependency().check_dependencies(event)


@catch_fail_exception
def trigger_next_dag(event):
    """
    Trigger the next DAG using the Trigger module.

    This function initializes and triggers the next DAG in the workflow
    using the provided event data. The `Trigger` class handles the DAG triggering.

    Args:
        event (dict): Dictionary containing event data required to trigger the next DAG.
    """
    Trigger().trigger_next_dag(event)


@catch_fail_exception
def start_dag(event):
    """
    Start a DAG using the process_dag module.

    This function initializes and starts a DAG with the provided event data.
    The `process_dag` module handles the starting of the DAG.

    Args:
        event (dict): Dictionary containing event data required to start the DAG.
    """
    process_dag().start_dag(event)


@catch_fail_exception
def stop_dag(event):
    """
    Stop and check a DAG using the process_dag module.

    This function initializes and stops a DAG with the provided event data.
    The `process_dag` module handles the stopping and checking of the DAG.

    Args:
        event (dict): Dictionary containing event data required to stop and check the DAG.
    """
    process_dag().dag_check(event)
