# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/3/11 17:57
"""
import abc
import random


# 解析sql语句，提取出所有table name

class SqlParseInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_element(self):
        pass


class GetTables(SqlParseInterface):
    def __init__(self, sql_statement=None):
        """
        :param sql_statement: sql 语句
        :return: all table names in tuple
        """
        self.sql_statement = sql_statement

    def get_element(self):
        table_names = ()
        return table_names
