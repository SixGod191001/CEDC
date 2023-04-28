from monitor import Monitor
from start import Start
from notify import Notify
from dependency import Dependency
from trigger import Trigger

"""
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
        Dependency().get_dependency_status(event)
        return "Completed dependency checking"
    except Exception as e:
        # insrt log here
        exit(1)


def trigger_next_dag(event):
    try:
        Trigger().trigger_dag(event)
        return "Successfully trigger next dag"
    except Exception as e:
        # insrt log here
        exit(1)
