# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/27 20:23
"""
from botocore.exceptions import ClientError
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig
import boto3_client
import json
from airflow.exceptions import AirflowFailException  # make the task failed without retry
from airflow.exceptions import AirflowException  # failed with retry
import logger_handler

logger = logger_handler.logger()


class SecretsManagerSecret:
    """Encapsulates Secrets Manager functions."""

    def __init__(self):
        """
        """
        self.secrets_manager_client = boto3_client.get_aws_boto3_client(service_name='secretsmanager')
        # secret_name = secret_name

    def get_value(self, secret_name=None, stage=None):
        """
        Gets the value of a secret.

        :param secret_name: name in secret manager
        :param stage: The stage of the secret to retrieve. If this is None, the
                      current stage is retrieved.
        :return: The value of the secret. When the secret is a string, the value is
                 contained in the `SecretString` field. When the secret is bytes,
                 it is contained in the `SecretBinary` field.
        """
        if secret_name is None:
            raise AirflowFailException("Couldn't get secret_name, ValueError!")

        try:
            kwargs = {'SecretId': secret_name}
            if stage is not None:
                kwargs['VersionStage'] = stage
            response = self.secrets_manager_client.get_secret_value(**kwargs)
            logger.info("Got value for secret %s.", secret_name)
        except ClientError:
            logger.exception("Couldn't get value for secret %s.", secret_name)
            raise AirflowException("Secret manager get_value function is bad! please check.")
        else:
            return response

    def get_cache_value(self, secret_name=None, stage=None):
        """
        :param secret_name: name in secret manager
        :param stage: stage in secret manager
        :return: return json string
        """

        if secret_name is None:
            raise ValueError

        try:
            kwargs = {'secret_id': secret_name}
            if stage is not None:
                kwargs['version_stage'] = stage
            cache_config = SecretCacheConfig()
            cache = SecretCache(config=cache_config, client=self.secrets_manager_client)
            response = cache.get_secret_string(**kwargs)
            logger.info("Got value for secret %s.", secret_name)
        except ClientError:
            logger.exception("Couldn't get value for secret %s.", secret_name)
            raise AirflowException("Secret manager get_cache_value function is bad! please check.")
        else:
            return response


if __name__ == "__main__":
    # secretsmanager = SecretsManagerSecret('cedc/dags/postgres')
    # sm_response = secretsmanager.get_value()
    # database_secrets = json.loads(sm_response['SecretString'])
    # print(database_secrets)
    # username = database_secrets['username']
    # password = database_secrets['password']
    # engine = database_secrets['engine']
    # host = database_secrets['host']
    # port = database_secrets['port']
    # dbname = database_secrets['dbname']
    secretsmanager = SecretsManagerSecret()
    sm_cache_response = secretsmanager.get_cache_value(secret_name='cedc/dags/postgres')
    result = json.loads(sm_cache_response)
    print(result)
