# -*- coding: utf-8 -*-
"""
@Author : Logan Xie
@Time : 2023/6/1 15:11
"""

# from airflow_workspace.module.monitor import Monitor
from airflow_workspace.module.Xie_monitor import Monitor
from airflow_workspace.utils.constants import Constants
from airflow_workspace.utils.postgre_handler import PostgresHandler
from airflow_workspace.utils.logger_handler import logger
"""
steps：
开始时间 >> 执行间隔 >> 判断 >> 终止glue job >> 写入数据库 >> 完成
"""
logger = logger()
a = ['a','b']
b = ['c']
c = a+b
logger.info("========= DAG FAILED : {p_dag} ===========".format(p_dag=c))


# print(Monitor.task_judgement('test'))
# glue_job_name = "cedc_sales_prelanding_job1"
# ph = PostgresHandler()
# dag_name = ph.get_record(Constants.SQL_GET_DAG_NAME.format())
# job_start_date = Monitor.get_job_name("task_cedc_sales_prelanding_push_params")
# print(jobs)
# for item in glue_job_list:
    # print(item)
# dag_name, task_names = Monitor.get_tasks_name('task_cedc_sales_a')
# print(dag_name)
# print(task_names)
# SQL = """
# SELECT DISTINCT task_name
# FROM fact_task_details
# WHERE dag_name='{}'
# """
# sql = """
# SELECT DISTINCT
# dag_name
# FROM fact_task_details
# WHERE task_name='{p_task_name}'
# """
# dag_name = ph.get_record(sql.format(p_task_name='task_cedc_sales_a'))[0]['dag_name']
# print(dag_name)
# ph.task_execute_update("task_cedc_department1_c",30,"TIMEOUT")

# SQL_GET_JOB_DATE = """
# select job_start_date
# from fact_job_details
# where job_name = '{job_name}'
# order by job_start_date DESC
# LIMIT 1;
# """
#
# SQL_GET_JOB_HANGINTERVAL = """
# select param_value
# from dim_job_params
# where job_name = '{job_name}'
# and param_name = 'interval';
# """
# # interval = float(SQL_GET_JOB_HANGINTERVAL)
# SQL_GET_JOB_RUNID = """
# # select run_id
# # from fact_job_details
# # where job_name = '{job_name}'
# # order by job_start_date DESC
# # LIMIT 1;
# # """
#
# """
# select job_start_date
# from fact_job_detail
# where job_name = cedc_sales_prelanding_job2
# order by job_start_date DESC
# LIMIT 1;
# """

# ph.dag_execute_update("dag_cedc_department2_g","FAILED")
# ph.task_execute_update("task_cedc_department1_c","SUCCESS")
# dag_name = ph.get_record(Constants.SQL_GET_DAG_NAME.format(
# "task_cedc_department1_c"))[0]['dag_name']
# print(dag_name)
# json_task_name = ph.get_record(Constants.SQL_GET_JOB_RUNID.format("cedc_sales_prelanding_job1"))[0]['run_id']
# print(json_task_name)
# sql = """
# SELECT DISTINCT job_name FROM fact_job_details WHERE task_name='task_cedc_sales_prelanding_push_params'
# """
# job_name = ph.get_record(sql)
# job_names = []
# for item in job_name:
#     job_names.append(item["job_name"])
# print(job_names)
#
# ph = PostgresHandler()
# start_date = ph.get_record(Constants.SQL_GET_JOB_DATE.format(
#     job_name=glue_job_name))[0]['job_start_date']
# # start_date = get_job_start_date[0]['job_start_date']
#
# job_interval = float(ph.get_record(Constants.SQL_GET_JOB_PARAM.format(
#     job_name=glue_job_name, param_name='interval'))[0]['param_value'])
# # print(interval)
#
# # job_interval = float(ph.get_record(SQL_GET_JOB_HANGINTERVAL.format(job_name=glue_job_name))[0]['param_value'])
# # job_interval = float(get_job_interval[0]['param_value'])
# now = datetime.now()
# formatted_now = now.strftime("%Y-%m-%d %H:%M:%S.%f")
# dt = datetime.strptime(formatted_now, "%Y-%m-%d %H:%M:%S.%f")
#
# time_out_deadline = start_date + timedelta(seconds=job_interval)
#
#
#
# if dt > time_out_deadline:
#     logger.info("===== JOB %s timeout, trying to kill it ======" % (glue_job_name))
# try:
#     # 停止Glue Job
#     glue = boto3.client("glue")
#     response = glue.stop_workflow_run(
#         Name=glue_job_name,
#         RunId=glue_job_name
#     )
#     logger.info("===== SUCCESSFULLY KILLED : %s ======" % glue_job_name)
# ph.execute_insert("jr_e030fff0be4041c2c2e25ceacfc9abbe815d7e8a942f33b29a393d0683b4bdc0", glue_job_name, status="TIME_OUT")

# # 比较date2_plus_1hour是否大于date1
# if dt > date1_plus_1hour:
#     logger.info("JOB %s timeout, trying to kill" % (glue_job_name))
#

#


#
# date1 = '2023-05-11 15:37:09.040'
# date2 = '2023-05-11 17:37:09.040'
#
# now = datetime.now()
# formatted_now = now.strftime("%Y-%m-%d %H:%M:%S.%f")
# dt = datetime.strptime(formatted_now, "%Y-%m-%d %H:%M:%S.%f")
# date_time_1 = datetime.strptime(SQL_GET_JOB_DATE, '%Y-%m-%d %H:%M:%S.%f')
# # print(type(dt))
#
# # 将date2加上3600秒（1小时）
# date1_plus_1hour = date_time_1 + timedelta(seconds=job_interval)
# # print(type(date1_plus_1hour))
# # date1_plus_1hour.strptime()
#
# # 比较date2_plus_1hour是否大于date1
# if dt > date1_plus_1hour:
#     logger.info("JOB %s timeout, trying to kill" % (glue_job_name))
#
#     try:
#         # 停止Glue Job
#         glue = boto3.client("glue")
#         response = glue.stop_workflow_run(
#             Name=glue_job_name,
#             RunId=glue_job_name
#         )
#         ph.execute_insert(run_id=None, job_name=glue_job_name, status="TIME_OUT")
#
#     except:
#         raise Exception("error occurs when stop glue job: {}".format(glue_job_name))
#
# # 停止glue
#
#
# glue = boto3.client('glue')
#
# response = glue.stop_job_run(
#     JobName='your-glue-job-name',
#     JobRunId='your-glue-job-run-id'
# )
#
# print(response)

# print(job_start_date)
