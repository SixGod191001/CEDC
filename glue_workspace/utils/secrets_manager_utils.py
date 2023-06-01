import boto3
import json
from botocore.exceptions import ClientError
from glue_workspace.utils.logger_handler import logger


class AWSSecretsManager:
    def __init__(self, region_name):
        self.region_name = region_name
        self.client = boto3.client('secretsmanager', region_name=self.region_name)

    def get_secret(self, secret_name):
        try:
            response = self.client.get_secret_value(SecretId=secret_name)
            logger.info("Got value for secret %s.", secret_name)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                raise ValueError(f"The secret '{secret_name}' was not found.")
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                raise ValueError("The request was invalid.")
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                raise ValueError("The request had invalid parameters.")
            else:
                raise ValueError("Failed to retrieve the secret.")

        if 'SecretString' in response:
            return response['SecretString']
        else:
            raise ValueError("Failed to retrieve the secret string.")


# 使用示例
if __name__ == "__main__":
    secrets_manager = AWSSecretsManager(region_name='ap-northeast-1')
    secret_value = secrets_manager.get_secret('cedc/dags/postgres')
    result = json.loads(secret_value)
    print(result)
