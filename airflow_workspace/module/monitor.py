# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/16 1:27
"""
import json
import time
from datetime import datetime, timedelta
from logging import info

import boto3
from airflow.exceptions import AirflowFailException
from botocore.exceptions import ClientError

from airflow_workspace.module.ThreadOverwrite import MyThread
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
        self.dag_id = ''
        self.event = ''
        self.task_name = ''
        self.load_type = ''
        self.run_type = 'glue'

    def monitor(self, event):
        """
        根据参数，监控不同job状态
        参数样例：'{"datasource_name": "sample",
                  "load_type": "ALL",
                  "run_type": "glue",
                  "glue_template_name":"cedc_sales_prelanding_template"}'
        """
        self.event = event
        self.task_name = event['task_name']
        self.dag_id = event['dag_id']
        # self.load_type = event['load_type']
        # self.run_type = event['run_type'
        # 根据不同的type调用不同的方法
        if self.run_type == 'glue':
            self.__dag_judgement()
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

    def __dag_judgement(self):
        """
        判断dag是否成功运行并写入数据库
        """

        # task_name = self.task_name

        # dag_id = glue_job['dag_id']
        # task_name = glue_job['task_name']
        #
        ph = PostgresHandler()
        # flag = []
        dag_name, task_names = Monitor.get_tasks_name(self.task_name)
        logger.info("======== dag_name: {}=========".format(dag_name))
        logger.info("======== task_name: {}=========".format(task_names))

        flag = self.__monitor_glues()

        # for item in task_names:
        #     judge = Monitor.__monitor_glues(item)
        #     flag.append(judge)
        if flag:
            logger.info("========= DAG SUCCEED : {p_dag} ===========".format(p_dag=dag_name))
            ph.dag_execute_update(dag_name, "SUCCESS")
            for item in task_names:
                ph.task_execute_update(item, "SUCCESS")
        else:
            logger.info("========= DAG FAILED : {p_dag} ===========".format(p_dag=dag_name))
            ph.dag_execute_update(dag_name, "FAILED")
            un_tasks = Monitor.un_success_task(dag_name)
            for item in un_tasks:
                ph.task_execute_update(item, "FAILED")
            logger.info("========= ERROR TASKS : {E_TASKS} ===========".format(E_TASKS=un_tasks))

    def __monitor_glues(self) -> bool:
        """
        # 监控dag下多个glue job状态
        监控Task下多个glue job状态
        """
        # 调用读取数据库的方法，获得当前dag的glue job的list
        ph = PostgresHandler()
        print("============ TASK_NAME  {}============".format(self.task_name))
        dag_name = ph.get_record(Constants.SQL_GET_DAG_NAME.format(self.task_name))[0]['dag_name']
        result = ph.get_record(Constants.SQL_GET_JOB_LIST.format(dag_name=dag_name))
        glue_job_list = []
        for item in result:
            new_dict = {'job_name': str(item['job_name']), 'run_id': str(item['run_id'])}
            glue_job_list.append(new_dict)
        logger.info("========= job list: {}==============".format(glue_job_list))
        # glue_job_list = [{'job_name': str(job_name), 'run_id': str(run_id)} for job_name, run_id in result]
        # glue_job_list = Monitor.get_job_name(self.task_name)
        # print(glue_job_list)
        job_state = []
        threads = []
        # 遍历glue job list，对每个job起一个线程进行监控
        for glue_job in glue_job_list:
            logger.info("============= job {} =========".format(glue_job))
            # thread = MyThread(func=Monitor.__monitor_glue, args=(glue_job['job_name'], glue_job['run_id']))
            thread = MyThread(func=self.__monitor_glue, args=(glue_job['job_name'], glue_job['run_id'],self.task_name))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
            job_state.append(thread.get_result())
        if all(elem == "SUCCESS" for elem in job_state):
            # ph.execute_update(run_id=glue_job_run_id, job_name=glue_job_name, status=job_state)
            ph.task_execute_update(self.task_name, "SUCCESS")
            logger.info("=========== ALL JOB SUCCEED ============")
            return True
        else:
            logger.info("=========== ERROR ============")
            ph.task_execute_update(self.task_name, "FAILED")
            return False

    def __monitor_glue(self, a,b,c) -> bool:
        """
        监控单个glue job状态
        :param glue_job: glue job 相关信息的dict
        """
        logger.info("=============== a: {}==============".format(a))
        ph = PostgresHandler()
        # glue_job_name = glue_job['job_name']
        # glue_job_run_id = glue_job['run_id']

        glue_job_name = a
        # logger.info("=============== a: {}==============".format(a))
        glue_job_name1 = ph.get_record(Constants.SQL_GET_JOB_TEMPLATE_NAME.format(c))[0]['job_template_name']

        glue_job_run_id = b
        # print(glue_job)
        logger.info("Job %s state check started.", glue_job_name1)
        # 调用读取数据库的方法，获得当前dag的glue job的monitor_interval和retry_limit，如没有返回值，则使用默认值

        monitor_interval = ph.get_record(Constants.SQL_GET_JOB_PARAM.format(
            job_name=glue_job_name, param_name='monitor_interval')) or 3
        retry_limit = ph.get_record(Constants.SQL_GET_JOB_PARAM.format(
            job_name=glue_job_name, param_name='retry_limit')) or 1
        # glue job 开始时间




        # flags = []

        # 获取glue job状态
        retry_times = 0
        job_state = Monitor.get_job_state_from_glue(glue_job_name1, glue_job_run_id)['JobRunState']
        logger.info("============= 1 state: {} ===========".format(job_state))

        for _ in range(retry_limit):
            # 当状态为ING时，等待monitor_interval后重新获取状态
            job_state = Monitor.time_out_judgement(glue_job_name,glue_job_name1,job_state,glue_job_run_id,monitor_interval)
            # while job_state in ['RUNNING', 'STARTING', 'STOPPING', 'WAITING']:
                # 判断，若glue超时将其停止，并插入数据库
                # logger.info(job_state)
                # if dt > time_out_deadline:
                #     # 停止job
                #     Monitor.stop_glue_job(glue_job_name1, [glue_job_run_id])
                #     ph.execute_update(glue_job_run_id,
                #                       glue_job_name, status="TIMEOUT")
                #
                # logger.info("Job %s is %s, wait for %d seconds to check again.",
                #             glue_job_name, job_state, monitor_interval)
                # dt += timedelta(seconds=monitor_interval)
                # time.sleep(monitor_interval)
                # job_state = Monitor.get_job_state_from_glue(glue_job_name1, glue_job_run_id)['JobRunState']
            if job_state in ['FAILED', 'TIMEOUT', 'ERROR']:
                if retry_times >= retry_limit:
                    # job执行状态写入数据库
                    ph.execute_update(run_id=glue_job_run_id, job_name=glue_job_name, status=job_state)
                    error_msg = Monitor.get_job_state_from_glue(glue_job_name1,glue_job_run_id)['ErrorMessage']
                    # glue_client = boto3_client.get_aws_boto3_client(service_name='glue')
                    # glue_job_response = glue_client.get_job_run(
                    #     JobName=glue_job_name1,
                    #     RunId=glue_job_run_id
                    # )
                    #
                    # error_msg = glue_job_response['JobRun']['ErrorMessage']
                    # # 抛出异常
                    raise AirflowFailException("Job %s is %s, error message: %s" %
                                               (glue_job_name, job_state, error_msg))
                    # return False
                else:
                    ph.execute_update(run_id=glue_job_run_id, job_name=glue_job_name, status=job_state)
                    logger.info("============ restart glue job: {} ==============".format(glue_job_name1))
                    new_job = Monitor.start_glue_job(glue_job_name1,glue_job_run_id)
                    glue_job_run_id = new_job['JobRunId']
                    # 重启的任务插入数据库
                    ph.execute_insert(glue_job_run_id,glue_job_name,"RUNNING")
                    retry_times += 1
                    logger.info("============ glue_job_name1: {} ===============".format(glue_job_name1))
                    logger.info("============ new glue_job_run_id: {} ===============".format(glue_job_run_id))
                    time.sleep(monitor_interval)
                    job_state = Monitor.get_job_state_from_glue(glue_job_name1, glue_job_run_id)['JobRunState']
                    job_state = Monitor.time_out_judgement(glue_job_name, glue_job_name1, job_state, glue_job_run_id,
                                                           monitor_interval)
                    logger.info("============= state2:{} ============".format(job_state))
                    logger.info("============= update to db: JOB: {} STATE:{} ============".format(glue_job_name,job_state))
                    # 所有job执行状态写入数据库
                    ph.execute_update(run_id=glue_job_run_id, job_name=glue_job_name, status=job_state)
            elif job_state == 'SUCCEEDED':
                ph.execute_update(run_id=glue_job_run_id, job_name=glue_job_name, status=job_state)
                return True
                # break
            elif job_state == 'STOPPED':
                ph.execute_update(run_id=glue_job_run_id, job_name=glue_job_name, status=job_state)
                break
        logger.info("Job %s is %s, state check completed", glue_job_name, job_state)

    @staticmethod
    def get_job_state_from_glue(job_name, run_id):
        """
        从glue中获取job的状态
        :return: job的状态
        """
        # glue_client = boto3_client.get_aws_boto3_client(service_name='glue', profile_name='ExecuteGlueService')
        glue_client = boto3_client.get_aws_boto3_client(service_name='glue', profile_name='ExecuteGlueService')
        # print(job_name, run_id)
        glue_job_response = glue_client.get_job_run(
            JobName=job_name,
            RunId=run_id
        )
        # glue_job_response = glue_client.get_job_run(JobName=job_name , RunId=run_id)
        return glue_job_response['JobRun']

    @staticmethod
    def get_job_state_from_db(job_name):
        """
        从数据库中获取job的状态
        :param job_name: job的名称
        :return: job的状态
        """
        psth = PostgresHandler()
        return psth.get_record(Constants.SQL_GET_LAST_GLUE_STATE.format(job_name=job_name))

    # @staticmethod
    # def stop_a_workflow(workflow_name, run_id):
    #     session = boto3.session.Session()
    #     glue_client = session.client('glue')
    #     try:
    #         response = glue_client.stop_workflow_run(Name=workflow_name, RunId=run_id)
    #         return response
    #     except ClientError as e:
    #         raise Exception("boto3 client error in stop_a_workflow: " + e.__str__())
    #     except Exception as e:
    #         raise Exception("Unexpected error in stop_a_workflow: " + e.__str__())

    @staticmethod
    def stop_glue_job(job_name, run_id, ):
        """
        停止glue job
        :return: void
        """
        logger.info("JOB %s timeout, trying to kill" % job_name)
        try:
            glue_client = boto3_client.get_aws_boto3_client(service_name='glue', profile_name='ExecuteGlueService')
            # print(job_name, run_id)
            glue_job_response = glue_client.batch_stop_job_run(
                JobName=job_name,
                JobRunIds=run_id
            )
            logger.info("===== SUCCESSFULLY KILLED : %s ======" % job_name)
            return glue_job_response
        except:
            raise Exception("ERROR: Error occurs when stopping glue job: %s" % job_name)
        # return glue_job_response


    @staticmethod
    def time_out_judgement(glue_job_name,glue_job_name1,job_state,glue_job_run_id,monitor_interval):
        ph = PostgresHandler()
        # 获取当前时间
        now = datetime.now()
        formatted_now = now.strftime("%Y-%m-%d %H:%M:%S.%f")
        dt = datetime.strptime(formatted_now, "%Y-%m-%d %H:%M:%S.%f")
        start_date = Monitor.get_job_state_from_glue(glue_job_name1,glue_job_run_id)['StartedOn']
        # start_date = ph.get_record(Constants.SQL_GET_JOB_DATE.format(
        #     job_name=glue_job_name))[0]['job_start_date']
        # 定义的glue job deadline

        interval = float(ph.get_record(Constants.SQL_GET_JOB_PARAM.format(
            job_name=glue_job_name, param_name='interval'))[0]['param_value'])
        interval = interval if interval is not None or interval != [] else 3600
        time_out_deadline = start_date + timedelta(seconds=interval)
        time_out_deadline = time_out_deadline.replace(tzinfo=None)
        dt = dt.replace(tzinfo=None)

        while job_state in ['RUNNING', 'STARTING', 'STOPPING', 'WAITING']:
            # 判断，若glue超时将其停止，并插入数据库
            print("========== dt type :{}=============".format(type(dt)))
            print("========== time_out_deadline type :{}=============".format(type(time_out_deadline)))
            if dt > time_out_deadline:
                # 停止job
                Monitor.stop_glue_job(glue_job_name1, [glue_job_run_id])
                ph.execute_update(glue_job_run_id,
                                  glue_job_name, status="TIMEOUT")

            logger.info("Job %s is %s, wait for %d seconds to check again.",
                        glue_job_name, job_state, monitor_interval)
            dt += timedelta(seconds=monitor_interval)
            time.sleep(monitor_interval)
            job_state = Monitor.get_job_state_from_glue(glue_job_name1, glue_job_run_id)['JobRunState']
        return job_state


    @staticmethod
    def un_success_task(dag_name):
        """
        根据dag name查出该dag下失败的task
        """
        list_task_name = []
        ph = PostgresHandler()
        json_task_names = ph.get_record(Constants.SQL_GET_FAILED_TASKS_NAME.format(dag_name))
        for item in json_task_names:
            list_task_name.append(item["task_name"])
        return list_task_name

    @staticmethod
    def start_glue_job(glue_job_name,run_id) -> dict:
        """
        失败重启glue job
        retuen： dict
        {'JobRunId': 'jr_1248e2bc85f4b1669789d61acd088d56d43c008b6f0104ee41885b72f78732b6', 'ResponseMetadata': {'RequestId': '2464e63c-fa14-43d0-8694-a5231013961b', 'HTTPStatusCode': 200, 'HTTPHeaders': {'date': 'Wed, 14 Jun 2023 06:53:29 GMT', 'content-type': 'application/x-amz-json-1.1', 'content-length': '82', 'connection': 'keep-alive', 'x-amzn-requestid': '2464e63c-fa14-43d0-8694-a5231013961b'}, 'RetryAttempts': 0}}
        """
        glue_client = boto3_client.get_aws_boto3_client(service_name='glue', profile_name='ExecuteGlueService')
        # print(job_name, run_id)
        glue_job_response = glue_client.get_job_run(
            JobName=glue_job_name,
            RunId=run_id
        )
        # print(glue_job_response)

        run_id = glue_job_response['JobRun']['Id']
        # #
        response = glue_client.start_job_run(
            JobName=glue_job_name,
            JobRunId=run_id
        )

        return response



    # return dag_name

    # for item in json_task_name:
    #     list_task_name.append(item["job_name"])
    # return list_task_name

    # @staticmethod
    # def task_judgement(task_name) -> bool:
    #     """
    #     遍历单个task的jobs，查看运行状态, 判断task状态并更新
    #     :return: Ture/False
    #     """
    #     # 根据task_name 找到glue job
    #     glue_job_name = Monitor.get_job_name(task_name)
    #     job_flag = None
    #     # task_flag = None
    #     # glue_job_name = glue_job['job_name']
    #     ph = PostgresHandler()
    #     error_jobs = []
    #
    #     for item in glue_job_name:
    #         # 找出最新的job run_id
    #         glue_job_run_id = ph.get_record(Constants.SQL_GET_JOB_RUNID.format(item))[0]['run_id']
    #
    #         logger.info("Jobs %s state check started.", item)
    #         # glue_job_run_id = glue_job["run_id"][glue_job_name.index(item)]
    #
    #         job_state = Monitor.get_job_state_from_glue(item, glue_job_run_id)
    #
    #         while job_state in ['RUNNING', 'STARTING', 'STOPPING', 'WAITING']:
    #             time.sleep(15)
    #             job_state = Monitor.get_job_state_from_glue(item, glue_job_run_id)
    #         if job_state in ['FAILED', 'TIMEOUT', 'ERROR']:
    #             error_jobs.append(item)
    #             ph.execute_update(run_id=glue_job_run_id, job_name=glue_job_name, status=job_state)
    #             job_flag = 0
    #         elif job_state == 'SUCCEEDED':
    #             ph.execute_update(run_id=glue_job_run_id, job_name=glue_job_name, status=job_state)
    #             job_flag = 1
    #         elif job_state == 'STOPPED':
    #             error_jobs.append(item)
    #             ph.execute_update(run_id=glue_job_run_id, job_name=glue_job_name, status=job_state)
    #             job_flag = 0
    #     if job_flag != 1:
    #         logger.info("========= TASK ERROR : {p_error_task} ===========".format(p_error_task=task_name))
    #         logger.info("========= Error Jobs: {p_error_jobs} ===========".format(p_error_jobs=error_jobs))
    #         ph.task_execute_update(task_name, "FAILED")
    #         return False
    #     else:
    #         logger.info("========= TASK SUCCEED : {p_task} ===========".format(p_task=task_name))
    #         ph.task_execute_update(task_name, "SUCCESS")
    #         return True

    @staticmethod
    def get_tasks_name(task_name):
        """
        根据传入的task name找出dag name以及该dag所有的task
        """
        ph = PostgresHandler()
        dag_name = ph.get_record(Constants.SQL_GET_DAG_NAME.format(
            task_name))[0]['dag_name']
        task_names = ph.get_record(Constants.SQL_GET_TASKS_NAME.format(
            dag_name))[0]['task_name']

        return dag_name, task_names


if __name__ == '__main__':
    # print('')
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
    event = {"task_name": "task_cedc_sales_prelanding_push_params",
             "dag_id": "cedc_airflow_monitor"
             }
    monitor.monitor(event)
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

    # monitor.get_job_state_from_glue('cedc_sales_prelanding_job1',
    #                                 'jr_436543d43ebea6dedc0588ab1a709b5e34a2408f9795ed8c7055630768739a31')
