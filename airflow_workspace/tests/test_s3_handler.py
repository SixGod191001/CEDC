import unittest
from airflow_workspace.utils import s3_handler


class Tests3Handle(unittest.TestCase):
    def tests3HandleSuccess(self):
        self.assertEquals(s3_handler.s3handler_usage_demo(),None)
