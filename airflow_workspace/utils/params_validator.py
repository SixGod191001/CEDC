# -*- coding: utf-8 -*-
"""
@Author : Logan Xie
@Time : 2023/5/8
"""

from airflow.exceptions import AirflowException

"""
dependency 参数验证
Parameters validator for dependency
"""
def dependency_validator(event):
    dag_ids = event['dag_ids']
    execution_date = event['execution_date']
    waiting_time = event['waiting_time']
    max_waiting_count = event['max_waiting_count']
    base_url = event['base_url']

    """
    判断参数是否为空
    Empty validator for parameters
    """
    params = [dag_ids, execution_date, waiting_time, max_waiting_count, base_url]
    for param in params:
        if not param:
            raise AirflowException("missing argument : {}".format(param))


"""
monitor 参数验证
Parameters validator for monitor
"""
def monitor_validator(event):
    datasource_name = event['datasource_name']
    load_type = event['load_type']
    run_type = event['run_type']

    '''
    判断任意一个参数是否为空，为空抛出AirflowException并停止
    throw AirflowException and stop itself while one of the parameters is empty
    '''
    params = [datasource_name, load_type, run_type]
    for param in params:
        if not param:
            raise AirflowException("missing argument : {}".format(param))

    """
    run_type 参数验证
    run_type validator
    """
    if run_type not in ['glue', 'spark', 'python', 'shell', 'procedure']:
        raise AirflowException("Wrong Type : run_type")


"""
notify 参数验证
Parameters validator for notify
"""
def notify_validator(event):
    job_name = event['datasource_name']

    '''
        判断任意一个参数是否为空，为空抛出AirflowException并停止
        throw AirflowException and stop itself while one of the parameters is empty
    '''
    if not job_name:
        raise AirflowException("missing argument : job_name")


"""
start 参数验证
Parameters validator for start
"""
def start_validator(event):
    """
    原本python代码中含有参数验证模块
    parameters will be validated in the original python scripts
    """
    pass


"""
trigger 参数验证
Parameters validator for trigger
"""
def trigger_validator(event):
    dag_run_id = event['dag_run_id']

    """
    判断任意一个参数是否为空，为空抛出AirflowException并停止
    throw AirflowException and stop itself while one of the parameters is empty
    """
    if not dag_run_id:
        raise AirflowException("missing argument dag_run_id")
