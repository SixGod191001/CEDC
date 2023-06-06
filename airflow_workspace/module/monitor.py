# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/16 1:27
"""
import threading
import time
from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException

from airflow_workspace.module.start import Start
from airflow_workspace.utils import boto3_client
from airflow_workspace.utils.constants import Constants
from airflow_workspace.utils.logger_handler import logger
from airflow_workspace.utils.postgre_handler import PostgresHandler

logger = logger()


class Monitor:
    def __init__(self):
        """
        监控glue job状态，当job状态为FAILED，TIMEOUT，ERROR时，重试。
        当job状态为ING状态时，等待monitor_interval后重新获取状态。值从数据库获取。
        当job状态为"FAILED，TIMEOUT，ERROR"，重试次数上限为retry_limit。值从数据库获取。
        """
        self.event = ''
        self.datasource_name = ''
        self.load_type = ''
        self.run_type = ''

    def monitor(self, event):
        """
        根据参数，监控不同job状态
        参数样例：'{"datasource_name": "sample",
                  "load_type": "ALL",
                  "run_type": "glue",
                  "glue_template_name":"cedc_sales_prelanding_template"}'
        """
        self.event = event
        self.datasource_name = event['datasource_name']
        self.load_type = event['load_type']
        self.run_type = event['run_type']
        # 根据不同的type调用不同的方法
        if self.run_type == 'glue':
            self.__monitor_glues()
        elif self.run_type == 'spark':
            pass
        elif self.run_type == 'python':
            pass
        elif self.run_type == 'shell':
            pass
        elif self.run_type == 'procedure':
            pass
        else:
            pass

    def __monitor_glues(self):
        """
        监控dag下多个glue job状态
        """
        # 调用读取数据库的方法，获得当前dag的glue job的list
        ph = PostgresHandler()
        # result = ph.get_record(Constants.SQL_GET_JOB_LIST.format(dag_name=self.datasource_name))
        # glue_job_list = [{'job_name': str(job_name), 'run_id': str(run_id)} for job_name, run_id in result]
        glue_job_list = ph.get_record(Constants.SQL_GET_JOB_LIST.format(dag_name=self.datasource_name))
        print(glue_job_list)
        threads = []
        # 遍历glue job list，对每个job起一个线程进行监控
        for glue_job in glue_job_list:
            thread = threading.Thread(target=self.__monitor_glue, args=(glue_job,))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

    def __monitor_glue(self, glue_job: dict):
        """
        监控单个glue job状态
        :param glue_job: glue job 相关信息的dict
        """
        glue_job_name = glue_job['job_name']
        glue_job_run_id = glue_job['run_id']
        print(glue_job)
        logger.info("Job %s state check started.", glue_job_name)
        # 调用读取数据库的方法，获得当前dag的glue job的monitor_interval和retry_limit，如果没有返回值，则使用默认值
        ph = PostgresHandler()
        monitor_interval = ph.get_record(Constants.SQL_GET_JOB_PARAM.format(
            job_name=glue_job_name, param_name='monitor_interval')) or 15
        retry_limit = ph.get_record(Constants.SQL_GET_JOB_PARAM.format(
            job_name=glue_job_name, param_name='retry_limit')) or 3
        # glue job 开始时间

        start_date = ph.get_record(Constants.SQL_GET_JOB_DATE.format(
            job_name=glue_job_name))[0]['job_start_date']
        # 定义的glue job deadline

        interval = float(ph.get_record(Constants.SQL_GET_JOB_PARAM.format(
            job_name=glue_job_name, param_name='interval'))[0]['param_value'])

        time_out_deadline = start_date + timedelta(seconds=interval)

        # 获取当前时间
        now = datetime.now()
        formatted_now = now.strftime("%Y-%m-%d %H:%M:%S.%f")
        dt = datetime.strptime(formatted_now, "%Y-%m-%d %H:%M:%S.%f")

        # 获取glue job状态
        retry_times = 0
        job_state = self.get_job_state_from_glue(glue_job_name, glue_job_run_id)
        for _ in range(retry_limit):
            # 当状态为ING时，等待monitor_interval后重新获取状态
            while job_state in ['RUNNING', 'STARTING', 'STOPPING', 'WAITING']:
                # 判断，若glue超时将其停止，并插入数据库
                if dt > time_out_deadline:
                    self.stop_glue_job(glue_job_name, glue_job_run_id)
                    ph.execute_insert(glue_job_run_id,
                                      glue_job_name, status="TIMEOUT")

                logger.info("Job %s is %s, wait for %d seconds to check again.",
                            glue_job_name, job_state, monitor_interval)
                dt += timedelta(seconds=monitor_interval)
                time.sleep(monitor_interval)
                job_state = self.get_job_state_from_glue(glue_job_name, glue_job_run_id)
            if job_state in ['FAILED', 'TIMEOUT', 'ERROR']:
                if retry_times > retry_limit:
                    # job执行状态写入数据库
                    ph.execute_update(run_id=glue_job_run_id, job_name=glue_job_name, status=job_state)
                    glue_client = boto3_client.get_aws_boto3_client(service_name='glue')
                    glue_job_response = glue_client.get_job_run(
                        JobName=glue_job_name,
                        RunId=glue_job_run_id
                    )
                    error_msg = glue_job_response['JobRun']['ErrorMessage']
                    # 抛出异常
                    raise AirflowFailException("Job %s is %s, error message: %s" %
                                               (glue_job_name, job_state, error_msg))
                else:
                    # 重试
                    s = Start()
                    glue_job_run_id = s.run_glue_job(glue_job_name)
                    retry_times += 1
                    job_state = self.get_job_state_from_glue(glue_job_name, glue_job_run_id)
                    # 所有job执行状态写入数据库
                    ph.execute_update(run_id=glue_job_run_id, job_name=glue_job_name, status=job_state)
            elif job_state == 'SUCCEEDED':
                ph.execute_update(run_id=glue_job_run_id, job_name=glue_job_name, status=job_state)
                break
            elif job_state == 'STOPPED':
                ph.execute_update(run_id=glue_job_run_id, job_name=glue_job_name, status=job_state)
                break
        logger.info("Job %s is %s, state check completed", glue_job_name, job_state)

    @staticmethod
    def get_job_state_from_glue(job_name, run_id, ):
        """
        从glue中获取job的状态
        :return: job的状态
        """
        glue_client = boto3_client.get_aws_boto3_client(service_name='glue', profile_name='ExecuteGlueService')
        print(job_name, run_id)
        glue_job_response = glue_client.get_job_run(
            JobName=job_name,
            RunId=run_id
        )
        return glue_job_response['JobRun']['JobRunState']

    @staticmethod
    def get_job_state_from_db(job_name):
        """
        从数据库中获取job的状态
        :param job_name: job的名称
        :return: job的状态
        """
        psth = PostgresHandler()
        return psth.get_record(Constants.SQL_GET_LAST_GLUE_STATE.format(job_name=job_name))

    @staticmethod
    def stop_glue_job(job_name, run_id, ):
        """
        停止glue job
        :return: void
        """
        logger.info("JOB %s timeout, trying to kill" % job_name)
        try:
            glue_client = boto3_client.get_aws_boto3_client(service_name='glue', profile_name='ExecuteGlueService')
            print(job_name, run_id)
            glue_job_response = glue_client.stop_workflow_run(
                Name=job_name,
                RunId=run_id
            )
            logger.info("===== SUCCESSFULLY KILLED : %s ======" % job_name)
        except:
            raise Exception("ERROR: Error occurs when stopping glue job: %s" % job_name)

    @staticmethod
    def get_job_name(task_name):
        list_task_name = []
        ph = PostgresHandler()
        json_task_name = ph.get_record(Constants.SQL_GET_JOB_NAME.format(task_name))
        for item in json_task_name:
            list_task_name.append(item["job_name"])
        return list_task_name

    @staticmethod
    def get_dag_name(task_name) -> list:
        # list_task_name = []
        ph = PostgresHandler()
        dag_name = ph.get_record(Constants.SQL_GET_DAG_NAME.format(task_name))[0]["dag_name"]
        # print(dag_name)

        return dag_name

        # for item in json_task_name:
        #     list_task_name.append(item["job_name"])
        # return list_task_name

    @staticmethod
    def task_judgement(task_name) -> bool:
        """
        遍历单个task的jobs，查看运行状态, 判断task状态并更新
        :return: Ture/False
        """
        # 根据task_name 找到glue job
        glue_job_name = Monitor.get_job_name(task_name)
        job_flag = None
        # task_flag = None
        # glue_job_name = glue_job['job_name']
        ph = PostgresHandler()
        error_jobs = []

        for item in glue_job_name:
            # 找出最新的job run_id
            glue_job_run_id = ph.get_record(Constants.SQL_GET_JOB_RUNID.format(item))[0]['run_id']

            logger.info("Jobs %s state check started.", item)
            # glue_job_run_id = glue_job["run_id"][glue_job_name.index(item)]

            job_state = Monitor.get_job_state_from_glue(item, glue_job_run_id)

            while job_state in ['RUNNING', 'STARTING', 'STOPPING', 'WAITING']:
                time.sleep(15)
                job_state = Monitor.get_job_state_from_glue(item, glue_job_run_id)
            if job_state in ['FAILED', 'TIMEOUT', 'ERROR']:
                error_jobs.append(item)
                ph.execute_update(run_id=glue_job_run_id, job_name=glue_job_name, status=job_state)
                job_flag = 0
            elif job_state == 'SUCCEEDED':
                ph.execute_update(run_id=glue_job_run_id, job_name=glue_job_name, status=job_state)
                job_flag = 1
            elif job_state == 'STOPPED':
                error_jobs.append(item)
                ph.execute_update(run_id=glue_job_run_id, job_name=glue_job_name, status=job_state)
                job_flag = 0
        if job_flag != 1:
            logger.info("========= TASK ERROR : {p_error_task} ===========".format(p_error_task=task_name))
            logger.info("========= Error Jobs: {p_error_jobs} ===========".format(p_error_jobs=error_jobs))
            ph.task_execute_update(task_name, "FAILED")
            return False
        else:
            logger.info("========= TASK SUCCEED : {p_task} ===========".format(p_task=task_name))
            ph.task_execute_update(task_name, "SUCCESS")
            return True

    @staticmethod
    def dag_judgement(task_name):
        """
        判断dag是否成功运行并写入数据库
        """
        ph = PostgresHandler()
        flag = []
        task_names = Monitor.get_tasks_name(task_name)
        dag_name = None

        for item in task_names:
            judge = Monitor.task_judgement(item)
            flag.append(judge)
        if False in flag:
            logger.info("========= DAG FAILED : {p_dag} ===========".format(p_dag=dag_name))
            ph.dag_execute_update(dag_name,"FAILED")
        else:
            logger.info("========= DAG SUCCEED : {p_dag} ===========".format(p_dag=dag_name))
            ph.dag_execute_update(dag_name, "SUCCESS")


    @staticmethod
    def get_tasks_name(task_name):
        """
        根据传入的task name找出dag name以及该dag所有的task
        """
        pass

        # if task_flag != 1:
        #     logger.info("========= TASK ERROR : {p_error_task} ===========".format(p_error_task=task_name))
        #     ph.task_execute_update(task_name,"FAILED")
        #     return False
        # else:
        #     logger.info("========= TASK SUCCESS : {p_task} ===========".format(p_task=task_name))
        #     ph.task_execute_update(task_name, "SUCCESS")
        #     return True
        #


if __name__ == '__main__':
    print('')
    # print('=================================测试monitor方法开始==================================')
    # import argparse
    # import json
    #
    # parser = argparse.ArgumentParser(description='Get variables from task in Airflow DAG')
    # parser.add_argument("--trigger", type=str, default='start_batch')
    # parser.add_argument("--params", type=str,
    #                     default='{"datasource_name": "dag_cedc_sales_prelanding", "load_type": "ALL", "run_type": "glue", '
    #                             '"glue_template_name":"cedc_sales_prelanding_template"}')
    # args = parser.parse_args()
    # print("params = " + args.params)
    # # convert json string to dict
    # batch_event = json.loads(args.params)
    # print(batch_event)
    # print("batch_event = " + str(batch_event))
    monitor = Monitor()
    #
    # print(monitor.monitor(batch_event))
    # print('=================================测试monitor方法结束==================================')
    #
    # print('=====================测试get_job_state_from_db方法开始=====================')
    # monitor = Monitor()
    # print(monitor.get_job_state_from_db('cedc_app1_prelanding_sales_user'))
    # print('=====================测试get_job_state_from_db方法结束=====================')

    # print('=====================测试__get_glue_job_state方法开始=====================')
    # print('=====================测试__get_glue_job_state方法结束=====================')

    monitor.get_job_state_from_glue('cedc_sales_prelanding_job1',
                                    'jr_436543d43ebea6dedc0588ab1a709b5e34a2408f9795ed8c7055630768739a31')
