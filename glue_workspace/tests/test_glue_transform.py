# -*- coding: utf-8 -*-
import os
import sys
import unittest
import re

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
from glue_workspace.script.glue_script_generator.transform import TransformGenerator


def remove_number(string):
    return re.sub(r'[0-9]+', '', string)


class Test_target(unittest.TestCase):
    def test_TransformGenerator(self):
        u = 'D:/work/CEDC/CEDC-master/test.sql'
        tg = TransformGenerator(u, ([
            '# Script generated for node employees\nemployees_node1948554591402 = glueContext.create_dynamic_frame.from_catalog(\n    database="devops",\n    table_name="employees",\n    transformation_ctx="employees_node1948554591402",\n)',
            '# Script generated for node departments\ndepartments_node1406226729501 = glueContext.create_dynamic_frame.from_catalog(\n    database="devops",\n    table_name="departments",\n    transformation_ctx="departments_node1406226729501",\n)']))
        transform_node, transformstr = tg.transform()
        self.assertIsNotNone(transform_node, transformstr)

    def test_sql_query(self):  # 读取文件
        data = 'select * from mytable where id = 2'
        u = 'D:/work/CEDC/CEDC-master/test.sql'
        tg = TransformGenerator(u, ([
            '# Script generated for node employees\nemployees_node1948554591402 = glueContext.create_dynamic_frame.from_catalog(\n    database="devops",\n    table_name="employees",\n    transformation_ctx="employees_node1948554591402",\n)',
            '# Script generated for node departments\ndepartments_node1406226729501 = glueContext.create_dynamic_frame.from_catalog(\n    database="devops",\n    table_name="departments",\n    transformation_ctx="departments_node1406226729501",\n)']))
        result = tg.sql_query()
        self.assertEqual(data, result)

    def test_create_dynamic_frame(self):
        u = 'D:/work/CEDC/CEDC-master/test.sql'
        tg = TransformGenerator(u, ([
            '# Script generated for node employees\nemployees_node1948554591402 = glueContext.create_dynamic_frame.from_catalog(\n    database="devops",\n    table_name="employees",\n    transformation_ctx="employees_node1948554591402",\n)',
            '# Script generated for node departments\ndepartments_node1406226729501 = glueContext.create_dynamic_frame.from_catalog(\n    database="devops",\n    table_name="departments",\n    transformation_ctx="departments_node1406226729501",\n)']))
        result_str = tg.sql_query()
        self.assertIsNotNone(result_str)
