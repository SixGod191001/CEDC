# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/27 20:23
"""
from botocore.client import logger
from botocore.exceptions import ClientError


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


if __name__ == "__main__":
    import boto3_client
    import json

    secretsmanager_client = boto3_client.get_aws_boto3_client(service_name='secretsmanager')
    secretsmanager = SecretsManagerSecret(secretsmanager_client, 'cedc/dags/postgres')
    response = secretsmanager.get_value()
    database_secrets = json.loads(response['SecretString'])
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

    # Decrypts secret using the associated KMS key.
