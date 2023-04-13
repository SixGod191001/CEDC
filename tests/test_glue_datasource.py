# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/13 23:08
"""
import unittest
import re
import os
from common.script.glue_script_generator.source import generate_datasource_interface, CsvDatasource


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

    def test_csv_datasource123(self):
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





