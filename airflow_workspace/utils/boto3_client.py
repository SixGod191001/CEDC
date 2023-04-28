import boto3
from botocore.client import logger
from botocore.exceptions import ClientError
from boto3.exceptions import Boto3Error


def get_aws_boto3_client(service_name=None, profile_name='airflow-role', region_name='ap-northeast-1'):
    """
    service_name:aws service name e.g 's3','secret manager'
    profile_name: awscli profile name
    region_name:aws region name e.g 'ap-northeast-1'
    """
    try:
        session = boto3.session.Session(profile_name=profile_name, region_name=region_name)
        client = session.client(service_name)
    except Boto3Error:
        logger.exception("Couldn't get profile %s.", profile_name)
        raise
    else:
        return client
