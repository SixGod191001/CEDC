# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/16 1:18
"""
"""
Purpose
Shows how to use the AWS SDK for Python (Boto3) with Amazon DynamoDB and PartiQL
to run batches of queries against a table that stores data about movies.
* Use batches of PartiQL statements to add, get, update, and delete data for
  individual movies.
"""
import logging
import sys
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class DynamoDBHandler:
    """
    Encapsulates a DynamoDB resource to run PartiQL statements.
    """

    def __init__(self, dyn_resource):
        """
        :param dyn_resource: A Boto3 DynamoDB resource.
        """
        self.dyn_resource = dyn_resource

    def get_items(self):
        print(f'当前类名称：{self.__class__.__name__}')
        print(f"当前方法名：{sys._getframe().f_code.co_name}")

    def create_items(self):
        print(f'当前类名称：{self.__class__.__name__}')
        print(f"当前方法名：{sys._getframe().f_code.co_name}")


    def update_items(self):
        print(f'当前类名称：{self.__class__.__name__}')
        print(f"当前方法名：{sys._getframe().f_code.co_name}")

    def delete_items(self):
        print(f'当前类名称：{self.__class__.__name__}')
        print(f"当前方法名：{sys._getframe().f_code.co_name}")



