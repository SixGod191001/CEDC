# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/3/9 22:23
"""
import abc
import random


# 生成 Glue数据源的代码

class DatasourceInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def create_dynamic_frame(self):
        pass


class CsvDatasource(DatasourceInterface):
    def __init__(self, database=None, table_name=None):
        """
        :param database: glue database name
        :param table_name: table name in glue
        :return:
        """
        self.database = database
        self.table_name = table_name
        self.transformation_ctx = "{table_name}_node{random_id}".format(table_name=table_name,
                                                                        random_id=random.randint(1000000000001,
                                                                                                 1999999999999))

    def create_dynamic_frame(self):
        comment = "# Script generated for node {NodeName}\n".format(NodeName=self.table_name)
        sql = '''{transformation_ctx} = glueContext.create_dynamic_frame.from_catalog(
    database="{database}",
    table_name="{table_name}",
    transformation_ctx="{transformation_ctx}",
)'''.format(database=self.database, table_name=self.table_name, transformation_ctx=self.transformation_ctx)
        return comment + sql


class RelationDBDatasource(DatasourceInterface):
    def create_dynamic_frame(self):
        pass


class RedshiftDatasource(DatasourceInterface):
    def create_dynamic_frame(self):
        pass


class MySQLDatasource(DatasourceInterface):
    def create_dynamic_frame(self):
        pass


class PostgreSQLDatasource(DatasourceInterface):
    def create_dynamic_frame(self):
        pass


class SQLServerDatasource(DatasourceInterface):
    def create_dynamic_frame(self):
        pass


class AmazonDynamoDatasource(DatasourceInterface):
    def create_dynamic_frame(self):
        pass


def generate_datasource(datasource_type):
    return datasource_type.create_dynamic_frame()


# 调用方法
source = generate_datasource(CsvDatasource(database='devops', table_name='user_csv'))
