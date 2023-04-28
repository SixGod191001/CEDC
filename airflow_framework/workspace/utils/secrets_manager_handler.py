# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/27 20:23
"""
from botocore.client import logger
from botocore.exceptions import ClientError
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig


class SecretsManagerSecret:
    """Encapsulates Secrets Manager functions."""

    def __init__(self, secrets_manager_client, secret_name):
        """
        :param secrets_manager_client: A Boto3 Secrets Manager client.
        :param secret_name: secret name.
        """
        self.secrets_manager_client = secrets_manager_client
        self.name = secret_name

    def get_value(self, stage=None):
        """
        Gets the value of a secret.

        :param stage: The stage of the secret to retrieve. If this is None, the
                      current stage is retrieved.
        :return: The value of the secret. When the secret is a string, the value is
                 contained in the `SecretString` field. When the secret is bytes,
                 it is contained in the `SecretBinary` field.
        """
        if self.name is None:
            raise ValueError

        try:
            kwargs = {'SecretId': self.name}
            if stage is not None:
                kwargs['VersionStage'] = stage
            response = self.secrets_manager_client.get_secret_value(**kwargs)
            logger.info("Got value for secret %s.", self.name)
        except ClientError:
            logger.exception("Couldn't get value for secret %s.", self.name)
            raise
        else:
            return response

    def get_cache_value(self, stage=None):
        """
        :param stage:
        :return: return json string
        """

        if self.name is None:
            raise ValueError

        try:
            kwargs = {'secret_id': self.name}
            if stage is not None:
                kwargs['version_stage'] = stage
            cache_config = SecretCacheConfig()
            cache = SecretCache(config=cache_config, client=self.secrets_manager_client)
            response = cache.get_secret_string(**kwargs)
            logger.info("Got value for secret %s.", self.name)
        except ClientError:
            logger.exception("Couldn't get value for secret %s.", self.name)
            raise
        else:
            return response


if __name__ == "__main__":
    import boto3_client
    import json

    secretsmanager_client = boto3_client.get_aws_boto3_client(service_name='secretsmanager')
    secretsmanager = SecretsManagerSecret(secretsmanager_client, 'cedc/dags/postgres')
    sm_response = secretsmanager.get_value()
    database_secrets = json.loads(sm_response['SecretString'])
    print(database_secrets)
    username = database_secrets['username']
    password = database_secrets['password']
    engine = database_secrets['engine']
    host = database_secrets['host']
    port = database_secrets['port']
    dbname = database_secrets['dbname']
    print('username is: {}, password is: {}, engine is: {}, host is: {}, port is: {}, dbname is: {}'.format(username,
                                                                                                            password,
                                                                                                            engine,
                                                                                                            host, port,
                                                                                                            dbname))
    sm_cache_response = secretsmanager.get_cache_value()
    result = json.loads(sm_cache_response)
    print(result)
