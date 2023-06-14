# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/5/6 15:40
"""
import unittest
from airflow_workspace.utils import secrets_manager_handler


class TestSecretsManagerHandler(unittest.TestCase):
    def test_get_value(self):
        self.assertIsNotNone(
            secrets_manager_handler.SecretsManagerSecret().get_value(secret_name='cedc/dags/postgres'))

    def test_get_cached_value(self):
        self.assertIsNotNone(
            secrets_manager_handler.SecretsManagerSecret().get_cache_value(secret_name='cedc/dags/postgres'))
