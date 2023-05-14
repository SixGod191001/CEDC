# -*- coding: utf-8 -*-
import unittest
from glue_workspace.script.glue_script_generator.sql_query_parse import GetTables


class Test_sql_query_parse(unittest.TestCase):
    def test_GetTables(self):
        data = {'table2', 'table1'}
        sql = 'SELECT * FROM table1 JOIN table2 ON table1.id = table2.id WHERE table1.name = "foo";'
        g = GetTables(sql)
        tables = g.get_element()
        self.assertEqual(data, tables)













