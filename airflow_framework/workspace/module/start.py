from airflow_framework.workspace.utils.db_handler import DynamoDBHandler
import sys
import boto3
from botocore.client import logger
from botocore.exceptions import ClientError

class Start:
    def __init__(self):
        self.dynamo_session = DynamoDBHandler(boto3.resource('dynamodb'))

    def run(self, event):
        """
        根据event里传入里类型调用具体执行run_glue, run_python还是其他
        :return:
        """
        print(f'当前类名称：{self.__class__.__name__}')
        print(f"当前方法名：{sys._getframe().f_code.co_name}")
        for k, v in event.items():
            setattr(self, k, v)
        if self.run_type == "glue":
            self.run_glue()
        elif self.run_type == "python":
            self.run_python()
        else:
            pass

    def run_glue(self):
        """
        可以一次run多个glue 根据后台返回的Job来判断
        :return:
        """
        glue_client = boto3.client('glue')
        print(f'当前类名称：{self.__class__.__name__}')
        print(f"当前方法名：{sys._getframe().f_code.co_name}")
        job_infos= {"sample_job1": {"ScriptLocation": "s3://training2223333/glue-script/demo1.py"}}
        for job_name, param in job_infos.items():
            # Need get last run status from database
            last_run_status='SUCCEED'
            if last_run_status not in ('RUNNING','WAITING') or last_run_status is None:
                jobid = self.start_job_run(glue_client, self.glue_template_name, param)
                if jobid is not None:
                    print(f"{job_name} is running, run id is {jobid}")
        # param = {"ScriptLocation": "s3://training2223333/glue-script/demo1.py"}
        # run_info = self.start_job_run(glue_client, self.glue_template_name, param)


    def run_python(self):
        print(f'当前类名称：{self.__class__.__name__}')
        print(f"当前方法名：{sys._getframe().f_code.co_name}")
    def start_job_run(self,glue_client,name, param):
        """
        :param name: The name of the glue  job.
        :return: The ID of the job run.
        """
        try:
            # The custom Arguments that are passed to this function are used by the
            # Python ETL script to determine the location of input and output data.
            response = glue_client.start_job_run(
                JobName=name,
                Arguments=param)
        except ClientError as err:
            logger.error(
                "Couldn't start job run %s. Here's why: %s: %s", name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['JobRunId']
if __name__ == "__main__":
    event={"datasource_name": "sample",
           "load_type": "ALL",
           "run_type": "glue",
            "glue_template_name":"devops.prelanding.s3_file_movement"}
    Start().run(event)
