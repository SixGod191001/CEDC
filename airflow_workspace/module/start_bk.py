# from airflow_framework.workspace.utils.db_handler import DynamoDBHandler
import sys
import boto3
from botocore.client import logger
from botocore.exceptions import ClientError
import json

from airflow_workspace.utils.constants import Constants
from airflow_workspace.utils.postgre_handler import PostgresHandler


class Start:
    def __init__(self):
        """
        :param event: sample value {"datasource_name": "sample", "load_type": "ALL", "run_type": "glue", "glue_template_name":"cedc_sales_prelanding_template"}
        """
        # self.dynamo_session = DynamoDBHandler(boto3.resource('dynamodb'))
        self.glue_client = boto3.client('glue')
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

    # def run_jobs(self,task_name):
    #     job_infos = self.query_job_by_task(task_name)
    #     for job_name, param in job_infos.items():

    def __run_glue(self):
        self.run_glue_task(self.task_name)

    def run_glue_task(self,task_name):
        """
        可以一次run多个glue 根据后台返回的Job来判断
        :return:
        """

        # job_infos = self.get_job_infos()
        job_infos = self.query_job_by_task(task_name)
        for  job_name in job_infos:
            param=job_infos[job_name]
            last_run_status = self.query_status_job(job_name)
            # last_run_status = 'SUCCEED'
            print("loop from ####  {job_name}")
            if last_run_status not in (Constants.GLUE_RUNNING,Constants.GLUE_WAITING) or last_run_status is None:
                run_id = self.start_glue_run(self.job_name, param)
                self.__start_batch(job_name,run_id)
                if run_id is not None:
                    print(f"{job_name} is running, run id is {run_id}")  # job_name  'sample_job1'
            else:
                print(f"{job_name} status not ready to start ")
    def run_glue_job(self,job_name):
        last_run_status = self.query_status_job(job_name)
        if last_run_status not in (Constants.GLUE_RUNNING,Constants.GLUE_WAITING) or last_run_status is None:
            job_info=self.query_job_info(job_name)
            run_id = self.start_glue_run(job_name, job_info[job_name])
            self.__start_batch(job_name,run_id)
        else:
            print(f"{job_name} status not ready to start ")
        return run_id

    def __run_python(self):
        pass

    def __start_batch(self,job_name,run_id):
        self.get_db_hander().execute_insert(run_id=run_id, job_name=job_name, status=Constants.GLUE_RUNNING)
        # INSERT_SQL='''INSERT INTO public.fact_job_details (dag_name,task_name,job_name,job_type,run_id,latest_heartbeat,job_start_date,job_end_date,duration,job_status,insert_date,last_update_date)
        #                     select t.dag_name ,t.task_name,job.job_name,job.job_type ,
        #                     '{p_run_id}' as run_id,
        #                     current_date as latest_heartbeat ,
        #                     current_date as job_start_date,
        #                     null as job_end_date,
        #                     0 as duration,
        #                     '{p_run_status}' as job_status,
        #                     current_date as insert_date,
        #                     current_date as last_update_date
        #                     from public.dim_job job
        #                     left join public.dim_task t on job.task_name  =t.task_name  and t.is_active ='Y'
        #                     where job.is_active ='Y'
        #                     and job.job_name ='{p_job_anme}'
        #                     '''

    def get_job_infos(self):
        # Hard code here, need get job and parameter from database
        params = {"database": "devops",
                  "target_path": "s3://training2223333/output/"}
        params_str = json.dumps(params)
        job_infos = {"sample_job1": {"--scriptLocation": "s3://training2223333/glue-script/demo.py",
                                     "--params": params_str}}
        return job_infos

    def start_glue_run(self, name, param):
        """
        :param glue_client: glue client.
        :param name: The name of the glue job.  exp  'devops.prelanding.s3_file_movement'
        :param param: The parameters, it should be a dict.
        :return: The ID of the job run.
        """
        try:
            # The custom Arguments that are passed to this function are used by the
            # Python ETL script to determine the location of input and output data.
            response = self.glue_client.start_job_run(
                JobName=name,  #  glue_template_name   'devops.prelanding.s3_file_movement'
                Arguments=param) #{'--scriptLocation': 's3://training2223333/glue-script/demo.py', '--params': '{"database": "devops", "target_path": "s3://training2223333/output/"}'}
        except ClientError as err:
            logger.error(
                "Couldn't start job run %s. Here's why: %s: %s", name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['JobRunId']

    def query_param(self,job_name='cedc_sales_prelanding_job1'):
        Query_SQL = """select param_name,param_value from public.dim_job_params
                            where job_name='{p_job_name}'
                            and is_active='Y'
                            and param_name <>'--scriptLocation'"""
        re = self.get_db_hander().get_record(Query_SQL.format(p_job_name=job_name))
        json_data = {}
        for result in re:
            json_data[result['param_name']] = result['param_value']
        # json_str = json.dumps(json_data)
        print(f"query_param  ####  {json_data}")
        return json_data
    def get_db_hander(self):
        # return PostgreHandler('postgreDB','postgres','password123','ec2-52-192-178-58.ap-northeast-1.compute.amazonaws.com','5432')
        return PostgresHandler()
    def query_job_by_task(self,task_name):
        jobs={}
        # get jobs by task name
        Query_SQL =""" select job_name from public.dim_job
                        where task_name='{p_task_name}'
                        and is_active='Y'
                        order by job_priority  """  #job_name like  'devops.prelanding.s3_file_movement'
        jobs_re=self.get_db_hander().get_record(Query_SQL.format(p_task_name=task_name))
        # for each job: get param
        for row in jobs_re:
            job = self.query_job_info(row['job_name'])
            jobs.update(job)
        return jobs
    def query_job_info(self,job_name):
        param_re = self.query_param(job_name)
        scriptLocation=self.query_scriptLocation(job_name)
        job = {job_name: {"--scriptLocation": scriptLocation,
                          "--params": json.dumps(param_re)}}
        print(f'one job_info ####  {job}')
        return job
    def query_scriptLocation(self,job_name='cedc_sales_prelanding_job1'):
        # scriptLocation="s3://training2223333/glue-script/demo.py"
        Query_SQL ="""select param_name,param_value from public.dim_job_params
                        where job_name='{p_job_name}'
                        and is_active='Y'
                        and param_name ='--scriptLocation' """  #job_name like  'devops.prelanding.s3_file_movement'
        re=self.get_db_hander().get_record(Query_SQL.format(p_job_name=job_name))
        scriptLocation = re[0]['param_value']
        print(f'scriptLocation ####  {scriptLocation}')
        return scriptLocation
    def query_status_job(self,job_name):
        Query_SQL = """ SELECT JOB_STATUS  FROM PUBLIC.FACT_JOB_DETAILS A
                            WHERE A.JOB_NAME = '{p_job_name}'
                            ORDER BY A.JOB_START_DATE DESC LIMIT 1"""
        re = self.get_db_hander().get_record(Query_SQL.format(p_job_name=job_name))
        last_run_status = re[0]['job_status']
        print(f'last_run_status   ####  {last_run_status}')
        return last_run_status
    def test_json(self, ):
        dd={}
        dd['p1'] = 'a'
        dd['p2'] = 2
        dd['p3'] = 'c'
        print(dd)
        print(json.dumps(dd))
        cc={'job_name':'j1','params':dd}
        print(cc)


if __name__ == "__main__":
    event= {"datasource_name": "sample",
           "load_type": "ALL",
           "run_type": "glue",
            "glue_template_name":"devops.prelanding.s3_file_movement"}
    # Start().run(event)
    # Start().query_param()
    # Start().test_json()
    # Start().query_job_info('cedc_sales_prelanding_job1')
    # Start().query_status_job('cedc_sales_prelanding_job1')
    # Start().run_glue_job('cedc_sales_prelanding_job1')
    Start().run_glue_task('task_cedc_sales_prelanding_push_params')
