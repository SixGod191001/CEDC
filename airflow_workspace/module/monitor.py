# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/16 1:27
"""

# import boto3
import time
from airflow_workspace.utils import boto3_client
from start import Start

# import retry


class Monitor:
    def __init__(self):
        """
        监控glue job状态，当job状态为FAILED，TIMEOUT，ERROR时，重试。
        当job状态为ING状态时，等待monitor_interval后重新获取状态。值从数据库获取。
        当job状态为"FAILED，TIMEOUT，ERROR"，重试次数上限为retry_limit。值从数据库获取。
        """
        self.job_state = ''
        self.error_msg = ''
        self.event = ''
        self.datasource_name = ''
        self.load_type = ''
        self.run_type = ''
        self.retry_times = 0

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
            self.monitor_glue()
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

    def monitor_glue(self):
        """
        监控glue job状态
            - 当job状态为SUCCEEDED时，记录日志。
            - 当job状态为STARTING，RUNNING，STOPPING，WAITING时，等待monitor_interval后再次获取状态。
            - 当job状态为FAILED，TIMEOUT，ERROR时，重试，重试次数上限为retry_limit。
            - 当job状态为STOPPED时，记录日志。
        """
        # glue_client = boto3.client('glue')
        glue_client = boto3_client.get_aws_boto3_client(service_name='glue')
        # 调用读取数据库的方法，获得当前dag的glue job的list
        """====================================     \/待实现\/开始\/     ===================================="""
        glue_job_list = [{'job_name': 'test', 'run_id': 'test'}]
        # glue_job_list = get_job_list(self.datasource_name)
        """====================================     /\待实现/\结束/\     ===================================="""
        # 遍历glue job list，对每个job进行监控
        for glue_job in glue_job_list:
            # 调用读取数据库的方法，获得当前dag的glue job的monitor_interval
            """====================================     \/待实现\/开始\/     ===================================="""
            monitor_interval = 15
            retry_limit = 3
            # monitor_interval = get_monitor_interval(glue_job['job_name'])
            # retry_limit = get_retry_limit(glue_job['job_name'])
            """====================================     /\待实现/\结束/\     ===================================="""
            self.__get_glue_job_state(glue_client, glue_job['job_name'], glue_job['run_id'])
            while self.job_state in ['RUNNING', 'STARTING', 'STOPPING', 'WAITING']:
                self.__get_glue_job_state(glue_client, glue_job['job_name'], glue_job['run_id'])
                time.sleep(monitor_interval)
                print('job state: ' + self.job_state + ', wait for ' + str(monitor_interval) + ' seconds')
            if self.job_state in ['FAILED', 'TIMEOUT', 'ERROR']:
                if self.retry_times > retry_limit:
                    print('retry times exceed retry limit, job failed. Error: ' + self.error_msg)
                    # 所有job执行状态写入数据库
                    """====================================     \/待实现\/开始\/     ===================================="""
                    # write_job_status(self.dag_name, self.job_state, self.error_msg)
                    """====================================     /\待实现/\结束/\     ===================================="""
                else:
                    # 调用读取数据库的方法，获得需要重试的glue job的参数
                    """====================================     \/待实现\/开始\/     ===================================="""
                    # param = get_param(glue_job['job_name'])
                    param = {"--scriptLocation": "s3://training2223333/glue-script/demo.py",
                             "--database": "devops",
                             "--target_path": "s3://training2223333/target/"}
                    """====================================     /\待实现/\结束/\     ===================================="""
                    s = Start()
                    glue_run_id = s.start_glue_run(glue_job['job_name'], param)
                    glue_job_state = self.__get_glue_job_state(glue_client, glue_job['job_name'], glue_run_id)
                    self.retry_times += 1
                    print('retry times: ' + str(self.retry_times))
                    # 所有job执行状态写入数据库
                    """====================================     \/待实现\/开始\/     ===================================="""
                    # write_job_status(self.dag_name,glue_job_state, glue_run_id)
                    """====================================     /\待实现/\结束/\     ===================================="""
            elif self.job_state == 'SUCCEEDED':
                print('job state: ' + self.job_state)
                # 所有job执行状态写入数据库
                """====================================     \/待实现\/开始\/     ===================================="""
                # write_job_status(self.dag_name, self.job_state, self.error_msg)
                """====================================     /\待实现/\结束/\     ===================================="""
            elif self.job_state == 'STOPPED':
                print('job state: ' + self.job_state)
                # 所有job执行状态写入数据库
                """====================================     \/待实现\/开始\/     ===================================="""
                # write_job_status(self.dag_name, self.job_state, self.error_msg)
                """====================================     /\待实现/\结束/\     ===================================="""

    def __get_glue_job_state(self, glue_client, glue_job_name, glue_run_id):
        """
        获得glue job的信息，包括job的状态和错误信息.
        :param glue_client: 通过boto3获得的glue client
        :param glue_job_name: glue job的名称
        :param glue_run_id: glue job的run_id
        """
        ''' ===============  临时用的state的假方法，用于测试  ==============='''
        self.job_state = self.__get_glue_job_state_fake()
        print('调用了假方法，job state: ' + self.job_state)
        ''' ===============  临时用的state的假方法，用于测试  ==============='''
        # glue_job_response = glue_client.get_job_run(
        #     JobName=glue_job_name,
        #     RunId=glue_run_id
        # )
        # self.job_state = glue_job_response['JobRun']['JobRunState']
        # self.error_msg = glue_job_response['JobRun']['ErrorMessage']
        # self.retry_times = 0
        return self.job_state

    @staticmethod
    def __get_glue_job_state_fake():
        """
        获取job的状态的假方法，用于测试
        :return:
        """
        import json
        # JobRunState': 'STARTING'|'RUNNING'|'STOPPING'|'STOPPED'|'SUCCEEDED'|'FAILED'|'TIMEOUT'|'ERROR'|'WAITING',
        response = '''
        {
    "JobRun": {
        "Id": "run_id",
        "JobName": "dag_name",
        "JobRunState": "SUCCEEDED"
    }
}'''
        response = json.loads(response)
        return response['JobRun']['JobRunState']

    @staticmethod
    def get_job_state_from_db(job_name):
        """
        从数据库中获取job的状态
        :param job_name: job的名称
        :return: job的状态
        """
        # 调用读取数据库的方法，获得当前dag的glue job的monitor_interval
        """====================================     \/待实现\/开始\/     ===================================="""
        state = 'SUCCEEDED'
        # state = get_state(glue_job['job_name'])
        """====================================     /\待实现/\结束/\     ===================================="""
        return state

# if __name__ == '__main__':
#     import argparse
#     import json
#
#     parser = argparse.ArgumentParser(description='Get variables from task in Airflow DAG')
#     parser.add_argument("--trigger", type=str, default='start_batch')
#     parser.add_argument("--params", type=str,
#                         default='{"datasource_name": "sample", "load_type": "ALL", "run_type": "glue", '
#                                 '"glue_template_name":"cedc_sales_prelanding_template"}')
#     args = parser.parse_args()
#
#     # convert json string to dict
#     batch_event = json.loads(args.params)
#     print("batch_event = " + str(batch_event))
#     monitor = Monitor(batch_event)
#
#     print(monitor.monitor())
