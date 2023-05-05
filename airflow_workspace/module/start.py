# from airflow_framework.workspace.utils.db_handler import DynamoDBHandler
import sys
import boto3
from botocore.client import logger
from botocore.exceptions import ClientError
from airflow_workspace.utils import boto3_client
import json
from datetime import datetime

class Start:
    def __init__(self):
        """
        :param event: sample value {"datasource_name": "sample", "load_type": "ALL", "run_type": "glue", "glue_template_name":"cedc_sales_prelanding_template"}
        """
        # self.dynamo_session = DynamoDBHandler(boto3.resource('dynamodb'))
        self.glue_client = boto3_client.get_aws_boto3_client(service_name='glue')

    def run(self, event):
        """
        根据event里传入里类型调用具体执行run_glue, run_python还是其他
        :return:
        """
        for k, v in event.items():
            setattr(self, k, v)

        if self.run_type == "glue":
            self.__run_glue()
        elif self.run_type == "python":
            self.__run_python()
        else:
            raise "Please specify correct run type"

    def __run_glue(self):
        """
        可以一次run多个glue 根据后台返回的Job来判断
        :return:
        """
        job_infos = self.get_job_infos()
        for job_name, param in job_infos.items():
            # Hard code here, need get last run status from database
            last_run_status = 'SUCCEED'
            if last_run_status not in ('RUNNING', 'WAITING') or last_run_status is None:
                jobid = self.start_glue_run(self.glue_template_name, param)
                if jobid is not None:
                    print(f"{job_name} is running, run id is {jobid}")

    def __run_python(self):
        pass

    def __start_batch(self):
        # Need insert a new batch id into database, and set the status as "Running"
        pass

    def get_job_infos(self):
        # Hard code here, need get job and parameter from database
        # s3 按照 年 > 月 > 日的文件夹分区结构存储目标文件
        d1 = datetime.today()
        sub_path = str(d1.year) + '/' + str(d1.month) + '/' + str(d1.day) + '/'

        params = {"database": "devops",
                  "target_path": "s3://training2223333/output/"}
        params["target_path"] = params["target_path"] + sub_path
        params_str = json.dumps(params)
        job_infos = {"sample_job1": {"--scriptLocation": "s3://training2223333/glue-script/demo.py",
                                     "--params": params_str}}
        return job_infos

    def start_glue_run(self, name, param):
        """
        :param glue_client: glue client.
        :param name: The name of the glue job.
        :param param: The parameters, it should be a dict.
        :return: The ID of the job run.
        """
        try:
            # The custom Arguments that are passed to this function are used by the
            # Python ETL script to determine the location of input and output data.
            response = self.glue_client.start_job_run(
                JobName=name,
                Arguments=param)
        except ClientError as err:
            logger.error(
                "Couldn't start job run %s. Here's why: %s: %s", name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['JobRunId']
# if __name__ == "__main__":
#     event= {"datasource_name": "sample",
#            "load_type": "ALL",
#            "run_type": "glue",
#             "glue_template_name":"devops.prelanding.s3_file_movement"}
#     Start().run(event)
