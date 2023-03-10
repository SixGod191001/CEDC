# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/3/9 22:23
"""

# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/3/9 22:23
"""
import abc
import random


# 生成 Glue Transform代码

class TransformInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def transform(self, sql_query):
        pass

    @abc.abstractmethod
    def sql_query(self):
        pass


class TransformGenerator(TransformInterface):
    def __init__(self, sql_path=None, datasource_node=()):
        """
        :param sql_path:
        :param datasource_node: tuple 参数传入不定个数的datasource node
        """
        self.sql_path = sql_path
        self.datasource_node = datasource_node

    def transform(self, sql_query):
        """
        :param sql_query: sql_query 方法返回的sql字符集
        :return:
        """
        pass

    def sql_query(self):
        """
        从sql文件读取sql内容，并返回sql内容，要求格式不丢失
        :return:
        """
        pass

# 调用方法
# source_ctx, source = generate_datasource_interface(CsvDatasource(database='devops', table_name='user_csv'))
