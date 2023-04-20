# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/16 1:27
"""
from airflow_framework.workspace.utils.db_handler import DynamoDBHandler
import sys
import boto3


class Start:
    def __init__(self):
        self.dynamo_session = DynamoDBHandler(boto3.resource('dynamodb'))

    def run(self, event):
        """
        根据event里传入里类型调用具体执行run_glue, run_python还是其他
        :return:
        """
        print(f'当前类名称：{self.__class__.__name__}')
        print(f"当前方法名：{sys._getframe().f_code.co_name}")

    def run_glue(self):
        """
        可以一次run多个glue 根据后台返回的Job来判断
        :return:
        """
        print(f'当前类名称：{self.__class__.__name__}')
        print(f"当前方法名：{sys._getframe().f_code.co_name}")

    def run_python(self):
        print(f'当前类名称：{self.__class__.__name__}')
        print(f"当前方法名：{sys._getframe().f_code.co_name}")
