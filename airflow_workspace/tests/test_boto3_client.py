# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/5/6 15:48
"""
import unittest
from airflow_workspace.utils import boto3_client


class TestBoto3ClientHandler(unittest.TestCase):
    def test_create_client(self):
        s3 = boto3_client.get_aws_boto3_client(service_name='s3')
        # Retrieve the list of existing buckets
        response = s3.list_buckets()
        self.assertIsInstance(response, cls=dict)
