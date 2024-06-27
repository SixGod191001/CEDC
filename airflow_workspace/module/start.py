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
    Class to manage and run multiple AWS Glue jobs based on provided event parameters by batch.
    Batch : Job = One : One/Many
    batch_name: Normally the datasource name
    """

    def __init__(self, event):
        """
        Initialize Start class with AWS Glue client.
        """
        self.event = event
        self.batch_name = None
        self.glue_client = get_aws_boto3_client(service_name='glue', profile_name=Constants.AWS_GLUE_ROLE)
        self.db_handler = PostgresHandler()

        for k, v in event.items():
            setattr(self, k, v)

    def __very_batch_status(self):
        pass

    def __get_job_details(self):
        """
        Retrieve job details from database based on batch name.

        Returns:
        - dict: A dictionary mapping job name to its details and parameters.
        """
        sql_get_job_details = Constants.SQL_GET_JOB_DETAILS.format(task_name=self.batch_name)
        try:
            job_details = self.db_handler.execute_select(sql_get_job_details)
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

    def __record_new_batch(self, status):
        """
        This function will record RUNNING status for new task in metadata database.
        If there is a RUNNING status record for this task, update the task status to FORCE_SUCCESS。
        This case is used to help ops team manually run the task which stuck in RUNNING status.
        Args:
        - status (str): Status of the task to be updated.
        """
        try:
            sql_get_running_batch = Constants.SQL_GET_RUNNING_TASK_NAME.format(task_name=self.batch_name)
            sql_insert_new_task = Constants.SQL_INSERT_TASK_DETAILS.format(task_name=self.batch_name, task_status=status)
            sql_force_success_task = Constants.SQL_FORCE_SUCCESS_TASK.format(task_status=Constants.FORCE_SUCCESS, task_name=self.batch_name)

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
        - event (dict): Event parameters containing task info
        - job_template_name: Which is the common empty glue job can run different jobs

        """
        try:

            job_details = self.__get_job_details()

            # Record new start task details into metadata database, also considering the retry logic
            self.__record_new_batch(Constants.GLUE_RUNNING)

            last_run_status_list = []

            for job_name in job_details.keys():
                last_run_status_list.append(self.query_job_status(job_name=job_name))  # [(run_id, job_status),(None, None)]
                logger.info(last_run_status_list)

            for job_name, info in job_details.items():
                param = {
                    '--scriptLocation': info['--scriptLocation'],
                    '--params': json.dumps(info['--params'])
                }
                for last_run_status in last_run_status_list:
                    if str(last_run_status[1]).upper() not in (Constants.GLUE_RUNNING, Constants.GLUE_WAITING) or last_run_status[1] is None:
                        # Which means brand-new case or not a RUNNING or WAITING case.
                        if info['job_type'] == 'glue':
                            run_id = self.run_glue_job(info["job_template_name"], param)
                            logger.info('Job run ID is {}'.format(run_id))
                            # insert a brand-new job into table - FACT_JOB_DETAILS.
                            self.db_handler.execute_sql(
                                Constants.SQL_INSERT_JOB_DETAILS.format(run_id=run_id, job_name=job_name, status=Constants.GLUE_RUNNING))
                    else:
                        # Force stop the last run glue job by job_name and run_id and run a new one
                        logger.info("{job_name} is running or waiting, run ID is {run_id}".format(job_name=job_name, run_id=last_run_status[0]))
                        response = self.stop_glue_job(job_name, last_run_status[0])
                        self.db_handler.execute_sql(Constants.SQL_INSERT_JOB_DETAILS.format(run_id=run_id, job_name=job_name, status=Constants.GLUE_RUNNING))
                        # Start a brand-new glue job after stop the last one that means full run all the jobs when retry
                        run_id = self.run_glue_job(info["job_template_name"], param)
                        self.__insert_new_job_details(job_name, run_id)

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

    def stop_glue_job(self, job_name, run_id):
        """
                Forcefully stop a running Glue Job.

                :param job_name: Name of the Glue Job to stop.
                :param run_id: ID of the running Glue Job to stop.
                :return: {
                            'SuccessfulSubmissions': [
                                {
                                    'JobName': 'string',
                                    'JobRunId': 'string'
                                },
                            ],
                            'Errors': [
                                {
                                    'JobName': 'string',
                                    'JobRunId': 'string',
                                    'ErrorDetail': {
                                        'ErrorCode': 'string',
                                        'ErrorMessage': 'string'
                                    }
                                },
                            ]
                        }
                :raises: ClientError if there is an error stopping the Glue Job.
        """
        try:
            response = self.glue_client.batch_stop_job_run(JobName=job_name, JobRunIds=run_id)
            # Check for any failed stop operations
            if 'Errors' in response and response['Errors']:
                for error in response['Errors']:
                    logger.error("Failed to stop Glue Job run: {}, Error: {}".format(error['JobRunId'], error['ErrorDetail']['ErrorMessage']))
            else:
                logger.info("Glue Job '{}' run ID '{}' successfully stopped.".format(job_name, run_id))
            return response
        except ClientError as e:
            logger.error("Couldn't start job run {job_name}. Error Message is: {error}".format(job_name=job_name, error=e))
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
            result = db_handler.execute_select(sql_get_latest_job_status)
            job_status = result[0]['job_status'] if result[0]['job_status'] else None
            run_id = result[0]['run_id'] if result[0]['run_id'] else None
            ret = (run_id, job_status)
            return ret
        except Exception as e:
            logger.error("Error querying job status from database: {}".format(str(e)))
            raise


if __name__ == "__main__":
    try:
        test_event = {"dag_id": "dag_cedc_sales_landing", "task_name": "task_cedc_sales_landing_loadning_data"}
        Start(test_event).run()
    except Exception as err:
        logger.error("Error in main execution: {}".format(str(err)))
        sys.exit(1)
