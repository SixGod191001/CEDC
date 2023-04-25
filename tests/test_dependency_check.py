import unittest
from datetime import datetime
from airflow_framework.workspace.module.dependency import Dependency

class TestDependency(unittest.TestCase):

    # 检查DAG已失败
    def test_get_dag_status_failed(self):
        checker = Dependency(dag_id='first_dag', execution_date=datetime(2023, 4, 22), waiting_time=4,
                             max_waiting_count=2)
        self.assertEqual(checker.get_dag_status(), 'failed')

   # 检查DAG已完成
    def test_get_dag_status(self):
        checker = Dependency(dag_id='first_dag', execution_date=datetime(2023, 4, 23), waiting_time=4,
                             max_waiting_count=2)
        self.assertEqual(checker.get_dag_status(), 'success')

    # 检查无DAG运行记录
    def test_get_dag_status_no_dag_run(self):
        checker = Dependency(dag_id='first_dag', execution_date=datetime(2023, 4, 25), waiting_time=4,
                             max_waiting_count=2)
        with self.assertRaises(ValueError):
            checker.get_dag_status()


