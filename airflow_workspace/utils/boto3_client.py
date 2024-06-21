import boto3
from botocore.exceptions import ClientError, Boto3Error
from airflow.exceptions import AirflowFailException, AirflowException
from airflow_workspace.utils.logger_handler import logger
from airflow_workspace.config.constants import Constants

logger = logger()

def get_aws_boto3_client(service_name=None, profile_name=None, region_name=Constants.AWS_REGION_NAME):
    """
    Return a boto3 client for the specified AWS service.

    Args:
    - service_name (str): AWS service name, e.g., 's3', 'secretsmanager', 'glue'.
    - profile_name (str): Optional. awscli profile name to use.
    - region_name (str): Optional. AWS region name, e.g., 'ap-northeast-1'.

    Returns:
    - boto3.client: Boto3 client object for the specified AWS service.

    Raises:
    - AirflowException: If there is an issue creating the boto3 client.
    """
    try:
        session = boto3.session.Session(profile_name=profile_name, region_name=region_name)
        client = session.client(service_name)
        return client
    except Boto3Error:
        logger.error("Couldn't get profile %s.", profile_name)
        raise AirflowException("Failed to create AWS boto3 client. Please check credentials and configuration.")

if __name__ == "__main__":
    # Example usage to list existing S3 buckets
    try:
        s3 = get_aws_boto3_client(service_name='s3')

        # Retrieve the list of existing buckets
        response = s3.list_buckets()

        # Output the bucket names
        print('Existing buckets:')
        for bucket in response['Buckets']:
            print(f'{bucket["Name"]}')
    except AirflowException as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise
