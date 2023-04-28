# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/28 8:44
"""
import unittest
from airflow_workspace.utils import secrets_manager_handler
import boto3
import json


class TestSecretsManager(unittest.TestCase):
    def test_get_sm_value(self):
        expectation = {'username': 'postgres', 'password': 'password123', 'engine': 'postgres',
                       'host': 'database-1.cw7feqnaopjp.ap-northeast-1.rds.amazonaws.com', 'port': '5432',
                       'dbname': 'postgreDB'}

        client = boto3.client('secretsmanager')
        secretsmanager = secrets_manager_handler.SecretsManagerSecret(client, 'cedc/dags/postgres')
        sm_response = secretsmanager.get_value()
        database_secrets = json.loads(sm_response['SecretString'])
        self.assertEqual(expectation, database_secrets)  # add assertion here

    def test_get_cached_sm_value(self):
        expectation = {'username': 'postgres', 'password': 'password123', 'engine': 'postgres',
                       'host': 'database-1.cw7feqnaopjp.ap-northeast-1.rds.amazonaws.com', 'port': '5432',
                       'dbname': 'postgreDB'}

        client = boto3.client('secretsmanager')
        secretsmanager = secrets_manager_handler.SecretsManagerSecret(client, 'cedc/dags/postgres')
        sm_cache_response = secretsmanager.get_cache_value()
        result = json.loads(sm_cache_response)
        self.assertEqual(expectation, result)  # add assertion here
