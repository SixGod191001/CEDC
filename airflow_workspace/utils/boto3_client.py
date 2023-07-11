import boto3
from botocore.exceptions import ClientError
from boto3.exceptions import Boto3Error
from airflow.exceptions import AirflowFailException  # make the task failed without retry
from airflow.exceptions import AirflowException  # failed with retry
from airflow_workspace.utils.logger_handler import logger

logger = logger()


def get_aws_boto3_client(service_name=None, profile_name=None, region_name='ap-northeast-1'):
    """
    service_name:aws service name e.g 's3','secret manager'
    profile_name: awscli profile name
    region_name:aws region name e.g 'ap-northeast-1'
    """
    try:
        session = boto3.session.Session(profile_name=profile_name, region_name=region_name)
        client = session.client(service_name)
    except Boto3Error:
        logger.error("Couldn't get profile %s.", profile_name)
        raise AirflowException("get_aws_boto3_client is bad! please check.")
    else:
        return client


if __name__ == "__main__":
    s3 = get_aws_boto3_client(service_name='s3')
    # Retrieve the list of existing buckets
    response = s3.list_buckets()

    # Output the bucket names
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')
