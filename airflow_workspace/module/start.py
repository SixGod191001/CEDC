# from airflow_framework.workspace.utils.db_handler import DynamoDBHandler
import sys
import boto3
from airflow import AirflowException
from botocore.client import logger
from botocore.exceptions import ClientError
import json
from airflow_workspace.utils.boto3_client import get_aws_boto3_client
from airflow_workspace.utils.constants import Constants
from airflow_workspace.utils.exception_handler import catch_fail_exception
from airflow_workspace.utils.postgre_handler import PostgresHandler
from airflow_workspace.utils.logger_handler import logger

logger = logger()

class Start:
    def __init__(self):
        """
        :param event: sample value {"datasource_name": "sample", "load_type": "ALL", "run_type": "glue", "glue_template_name":"cedc_sales_prelanding_template"}
        """
        self.glue_client = get_aws_boto3_client(service_name='glue',profile_name='ExecuteGlueService')
    def __get_job_infos(self):
        """
        :param self.task_name: The task name.
        :return: A dictionary, include job names and corresponding parameters.
        """
        query_sql=f"""
        select j.job_name ,j.job_type ,j.job_priority ,j.max_retries ,j.load_type ,j.job_template_name,j.s3_location as "--scriptLocation"
        ,p.param_name || ' param_value '||p.param_value as param_value
        from public.dim_job j 
        inner join public.dim_job_params p on j.job_name =p.job_name 
        where j.task_name ='{self.task_name}' 
        order by job_priority ,job_name
        """
        db_hander = PostgresHandler()
        job_infos_all = db_hander.get_record(query_sql)
        jobs = {}
        job_info = {}
        params = {}
        for job in job_infos_all:
            for param, value in job.items():
                if param == 'job_name':
                    jobs[value] = job_info
                    job_info['--params'] = params
                elif param == 'param_value':
                    param_value = value.split(' param_value ')
                    params[param_value[0]] = param_value[1]
                else:
                    job_info[param] = value
            job_info = {}

        return jobs

    def run(self,event):
        """
        可以一次run多个glue 根据后台返回的Job来判断
        :return:
        """
        for k, v in event.items():
            setattr(self, k, v)
        job_infos = self.__get_job_infos()
        self.__start_task(Constants.GLUE_RUNNING)

        # print(job_infos)

        for job_name in job_infos.keys():
            last_run_status_list = []
            last_run_status_list.append(self.query_status_job(job_name=job_name))
            print(last_run_status_list)
        for job_name, info in job_infos.items():
            #last_run_status = self.query_status_job(job_name=job_name)
            #last_run_status = 'SUCCEED'
            param={}
            param['--scriptLocation']=info['--scriptLocation']
            param['--params']=json.dumps(info['--params'])
            for last_run_status in last_run_status_list:
                if last_run_status not in (Constants.GLUE_RUNNING,Constants.GLUE_WAITING,'running','waiting') or last_run_status is None:
                    if info['job_type'] == 'glue':
                        run_id = self.start_glue_run(info["job_template_name"],param)
                        print(f'job run id is {run_id}')
                    self.__update_job_details(job_name,run_id)
                    if run_id is not None:
                        logger.info(f"{job_name} is running, run id is {run_id}")  # job_name  'sample_job1'
                else:
                    logger.info(f"{job_name} status not ready to start ")

    def __run_python(self):
        pass

    def __update_job_details(self,job_name,run_id):
        db_hander = PostgresHandler()
        db_hander.execute_insert(run_id=run_id, job_name=job_name, status=Constants.GLUE_RUNNING)

    @catch_fail_exception
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

    def query_status_job(self,job_name):
        Query_SQL = """ select job_status from fact_job_details
                        where job_name = '{job_name}'
                        order by last_update_date desc limit 1;"""
        db_hander = PostgresHandler()
        job_status = db_hander.get_record(Query_SQL.format(job_name=job_name))
        last_run_status = job_status[0]['job_status'] if job_status != [] else None

        return last_run_status

    def __start_task(self,status):
        # 执行insert_fact_task
        query_sql=f"""
        select  task_name from public.fact_task_details
        where task_name='{self.task_name}' and lower(status)='running'
        """
        insert_sql = """ insert into fact_task_details 
                                         (task_name,dag_name,execution_date,start_date,end_date,duration,run_id,status,retry_number,priority_weight ,max_tries ,insert_date,last_update_date)
                                         select 
                                             '{task_name}' as task_name,
                                             task.dag_name,
                                             current_timestamp as execution_date ,
                                             current_timestamp as start_date ,
                                             null as end_date ,
                                             null as duration,
                                             null as run_id ,
                                             '{task_status}' as status,
                                             3 as retry_number ,
                                             task.priority_weight ,
                                             task.max_tries ,
                                             current_timestamp as insert_date ,
                                             current_timestamp as last_update_date 
                                             from dim_task task
                                             inner join dim_dag dag on task.dag_name = dag.dag_name 
                                              where task.task_name = '{task_name}'
                                     """.format(task_name=self.task_name, task_status=status)
        update_sql=f""" update fact_task_details 
        set end_date=current_timestamp,
        duration=null,
        status='{Constants.FORCE_SUCCESS}'
        last_update_date=current_timestamp
        where task_name='{self.task_name}' and lower(status)='running'
        """
        running_task = PostgresHandler().get_record(query_sql)
        if running_task is not None or running_task != []:
            PostgresHandler().execute_sql(insert_sql)
        else:
            PostgresHandler().execute_sql(update_sql)
            PostgresHandler().execute_sql(insert_sql)


    def run_glue_job(self, glue_job_name):
        pass

if __name__ == "__main__":
    event= {"dag_id":"dag_cedc_sales_landing","task_name": "task_cedc_sales_landing_loadning_data"}
    Start().run(event)

    # dag_name = {"dag_name":"dag_cedc_department1_f"}
    # Start().query_fact_task_insert(dag_name=dag_name,task_status=Constants.GLUE_RUNNING)
