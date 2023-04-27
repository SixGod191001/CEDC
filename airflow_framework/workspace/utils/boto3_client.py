from boto3 import Session

def get_aws_boto3_client(service_name=None, region_name='ap-northeast-1'):
    """
    service_name:aws service name e.g 's3','secret manager'
    region_name:aws region name e.g 'ap-northeast-1'
    """
    boto3_session = Session(profile_name='airflow-role', region_name=region_name)
    client = boto3_session.client(service_name)
    return client
