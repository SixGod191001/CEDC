import sys
import json
from botocore.exceptions import ClientError
from airflow_workspace.utils.boto3_client import get_aws_boto3_client
from airflow_workspace.config.constants import Constants
from airflow_workspace.utils.exception_handler import catch_fail_exception
from airflow_workspace.utils.postgre_handler import PostgresHandler
from airflow_workspace.utils.logger_handler import get_logger

logger = get_logger()


class Start:
    """
    Class to manage and run multiple AWS Glue jobs based on provided event parameters.
    """

    def __init__(self, event):
        """
        Initialize Start class with AWS Glue client.
        """
        self.event = event
        self.task_name = None
        self.glue_client = get_aws_boto3_client(service_name='glue', profile_name='ExecuteGlueService')

        for k, v in event.items():
            setattr(self, k, v)

    def __get_job_details(self):
        """
        Retrieve job details from database based on task name.

        Returns:
        - dict: A dictionary mapping job names to their details and parameters.
        """
        sql_get_job_details = Constants.SQL_GET_JOB_DETAILS.format(task_name=self.task_name)
        try:
            db_handler = PostgresHandler()
            job_details = db_handler.execute_select(sql_get_job_details)
            jobs = {}

            for job in job_details:
                job_info = {'--params': {}}
                for param, value in job.items():
                    if param == 'job_name':
                        jobs[value] = job_info
                    elif param == 'param_value':
                        param_value = value.split(' param_value ')
                        job_info['--params'][param_value[0]] = param_value[1]
                    else:
                        job_info[param] = value

            return jobs
        except Exception as e:
            logger.error("Error fetching job details from database: {}".format(str(e)))
            raise

    @staticmethod
    def __update_job_details(job_name, run_id):
        """
        Update job details in database with the current job run ID.

        Args:
        - job_name (str): Name of the job.
        - run_id (str): ID of the current job run.
        """
        try:
            db_handler = PostgresHandler()
            db_handler.execute_insert(run_id=run_id, job_name=job_name, status=Constants.GLUE_RUNNING)
        except Exception as e:
            logger.error("Error updating job details in database: {}".format(str(e)))
            raise

    def __record_new_tasks(self, status):
        """
        This function will record RUNNING status for new task in metadata database.
        If there is a RUNNING status record for this task, update the task status to FORCE_SUCCESS。

        Args:
        - status (str): Status of the task to be updated.
        """
        try:
            sql_get_running_task = Constants.SQL_GET_RUNNING_TASK_NAME.format(task_name=self.task_name)
            sql_insert_new_task = Constants.SQL_INSERT_TASK_DETAILS.format(task_name=self.task_name, task_status=status)
            sql_force_success_task = Constants.SQL_FORCE_SUCCESS_TASK.format(task_status=Constants.FORCE_SUCCESS, task_name=self.task_name)

            db_handler = PostgresHandler()
            running_task = db_handler.execute_select(sql_get_running_task)
            if running_task:
                db_handler.execute_sql(sql_force_success_task)
            db_handler.execute_sql(sql_insert_new_task)
        except Exception as e:
            logger.error("Error updating task details: {}".format(str(e)))
            raise

    def run(self):
        """
        Execute multiple Glue jobs based on the provided event parameters.

        Args:
        - event (dict): Event parameters containing datasource_name, load_type, run_type, and glue_template_name.

        """
        try:
            # Required parameters validation
            required_keys = ["datasource_name", "load_type", "run_type", "glue_template_name"]
            for key in required_keys:
                if key not in self.event:
                    raise ValueError("Missing required parameter: {}".format(key))

            job_details = self.__get_job_details()

            # Record new start task details into metadata database
            self.__record_new_tasks(Constants.GLUE_RUNNING)

            last_run_status_list = None

            for job_name in job_details.keys():
                last_run_status_list = [self.query_job_status(job_name=job_name)]
                logger.info(last_run_status_list)

            for job_name, info in job_details.items():
                param = {
                    '--scriptLocation': info['--scriptLocation'],
                    '--params': json.dumps(info['--params'])
                }
                for last_run_status in last_run_status_list:
                    run_id = None
                    if str(last_run_status).upper() not in (Constants.GLUE_RUNNING, Constants.GLUE_WAITING) or last_run_status is None:
                        if info['job_type'] == 'glue':
                            run_id = self.run_glue_job(info["job_template_name"], param)
                            logger.info('Job run ID is {}'.format(run_id))
                        self.__update_job_details(job_name, run_id)
                        if run_id is not None:
                            logger.info("{job_name} is running, run ID is {run_id}".format(job_name=job_name, run_id=run_id))
                    else:
                        logger.info("{} status not ready to start".format(job_name))
        except Exception as e:
            logger.error("Error running the jobs: {}".format(str(e)))
            raise

    @catch_fail_exception
    def run_glue_job(self, name, param):
        """
        Start a Glue job run.

        Args:
        - name (str): Name of the Glue job.
        - param (dict): Parameters for the Glue job run.

        Returns:
        - str: ID of the job run.
        """
        try:
            response = self.glue_client.start_job_run(JobName=name, Arguments=param)
            return response['JobRunId']
        except ClientError as e:
            logger.error("Couldn't start job run {name}. Error Message is: {error}".format(name=name, error=e))
            raise

    @staticmethod
    def query_job_status(job_name):
        """
        Query the latest job status from the database.

        Args:
        - job_name (str): Name of the job.

        Returns:
        - str or None: Job status or None if no status is found.
        """
        try:
            sql_get_latest_job_status = Constants.SQL_GET_LATEST_JOB_RUN_STATUS.format(job_name=job_name)
            db_handler = PostgresHandler()
            job_status = db_handler.execute_select(sql_get_latest_job_status)
            return job_status[0]['job_status'] if job_status else None
        except Exception as e:
            logger.error("Error querying job status from database: {}".format(str(e)))
            raise


if __name__ == "__main__":
    try:
        test_event = {"datasource_name": "sample", "load_type": "ALL", "run_type": "glue", "glue_template_name": "cedc_sales_prelanding_template"}
        Start(test_event).run()
    except Exception as err:
        logger.error("Error in main execution: {}".format(str(err)))
        sys.exit(1)
