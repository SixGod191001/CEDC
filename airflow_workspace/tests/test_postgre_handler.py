import unittest
from airflow_workspace.utils import postgre_handler
import json
from airflow_workspace.utils.secrets_manager_handler import SecretsManagerSecret

"""
最新postgre info
HOST: ec2-52-192-178-58.ap-northeast-1.compute.amazonaws.com
PORT: 5432
DB Name: postgres
USERNAME: postgresadm
PASSWORD: password123
"""


class Test_postgre_handler(unittest.TestCase):
    # 全局变量
    global check
    check = postgre_handler.PostgresHandler()

    def test_get_record(self):
        sql = "select * from dim_job_params;"
        res = check.get_record(self,sql)
        self.assertIsNotNone(res)

    def test_execute_insert(self):
        self.assertIsNotNone(check.execute_insert(run_id="2",job_name="cedc_sales_prelanding_job1",status="running"))

    def test_execute_update(self):
        self.assertIsNotNone(check.execute_update(run_id="2",job_name="cedc_sales_prelanding_job1",status="running"))

    def test_execute_delete(self):
        self.assertIsNotNone(check.execute_delete(run_id="2"))