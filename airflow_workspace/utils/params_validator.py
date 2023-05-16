# -*- coding: utf-8 -*-
"""
@Author : Logan Xie
@Time : 2023/5/8
"""

from airflow.exceptions import AirflowException


def dependency_validator(event):
    """
    dependency 参数验证
    Parameters validator for dependency
    """
    dag_id = event['dag_id']
    # execution_date = event['execution_date']
    waiting_time = event['waiting_time']
    max_waiting_count = event['max_waiting_count']
    base_url = event['base_url']

    """
    判断参数是否为空
    Empty validator for parameters
    """
    params = [dag_id, waiting_time, max_waiting_count, base_url]
    for param in params:
        if not param:
            raise AirflowException("missing argument : {}".format(param))


def monitor_validator(event):
    """
    monitor 参数验证
    Parameters validator for monitor
    """
    datasource_name = event['datasource_name']
    load_type = event['load_type']
    run_type = event['run_type']
    glue_template = event['glue_template_name']

    # run_type为glue时，验证glue template是否存在
    # Validating the glue template while run_type == 'glue'
    if run_type == 'glue' and not glue_template:
        raise AirflowException("Glue template name missing")

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


def notify_validator(event):
    """
    notify 参数验证
    Parameters validator for notify
    """
    job_name = event['datasource_name']

    '''
        判断任意一个参数是否为空，为空抛出AirflowException并停止
        throw AirflowException and stop itself while one of the parameters is empty
    '''
    if not job_name:
        raise AirflowException("missing argument : job_name")


def start_validator(event):
    """
    start 参数验证
    Parameters validator for start
    """
    datasource_name = event['datasource_name']
    load_type = event['load_type']
    glue_template = event['glue_template_name']

    """
    判断任意一个参数是否为空，为空抛出AirflowException并停止
    throw AirflowException and stop itself while one of the parameters is empty
    源代码中含有run_type参数验证模块
    run_type have already been validated in the original python script.
    """
    params = [datasource_name, load_type, glue_template]
    for param in params:
        if not param:
            raise AirflowException("missing argument : {}".format(param))


def trigger_validator(event):
    """
    trigger 参数验证
    Parameters validator for trigger
    """
    dag_run_id = event['dag_run_id']
    url = event['url']
    array_list = [dag_run_id, url]

    """
    判断任意一个参数是否为空，为空抛出AirflowException并停止
    throw AirflowException and stop itself while one of the parameters is empty
    """
    for item in array_list:
        if not item:
            raise AirflowException("missing argument dag_run_id")
