# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/13 23:08
"""
import unittest
import re
import os
from glue_workspace.script.glue_script_generator.source import generate_datasource_interface, CsvDatasource,PostgreSQLDatasource,SQLServerDatasource


def remove_number(string):
    return re.sub(r'[0-9]+', '', string)


class TestSource(unittest.TestCase):
    def test_csv_datasource(self):
        data = ['user_csv_node',
                '''# Script generated for node user_csv
user_csv_node = glueContext.create_dynamic_frame.from_catalog(
    database="devops",
    table_name="user_csv",
    transformation_ctx="user_csv_node",
)''']
        # 调用方法
        source_ctx, source = generate_datasource_interface(CsvDatasource(database='devops', table_name='user_csv'))
        self.assertEqual(data, [remove_number(source_ctx), remove_number(source)])



    def test_SQLServer_Datasource(self):
        data = ['DimUserSourceNode_node',
                '''# Script generated for node DimUserSourceNode
DimUserSourceNode_node = glueContext.create_dynamic_frame.from_catalog(
            database="devops",
            table_name="cedc_dbo_dimuser",
            transformation_ctx="DimUserSourceNode_node",
)''']
        # 调用方法
        source_ctx, source = generate_datasource_interface(SQLServerDatasource(database='devops', table_name='cedc_dbo_dimuser'))
        self.assertEqual(data, [remove_number(source_ctx), remove_number(source)])



    def test_PostgreSQL_Datasource(self):
        data = ['post_cedc_dbo_dimuser_node',
                '''# Script generated for node PostgreSQL
post_cedc_dbo_dimuser_node = glueContext.create_dynamic_frame.from_catalog(
    database="devops",
    table_name="post_cedc_dbo_dimuser",
    transformation_ctx="post_cedc_dbo_dimuser_node",
)''']
        # 调用方法
        source_ctx, source = generate_datasource_interface(PostgreSQLDatasource(database='devops', table_name='post_cedc_dbo_dimuser'))
        self.assertEqual(data, [remove_number(source_ctx), remove_number(source)])