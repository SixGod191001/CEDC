import unittest
from datetime import datetime
from airflow_workspace.module.dependency import Dependency


class TestDependency(unittest.TestCase):

    # 检查DAG已失败
    def test_check_dag_status_failed(self):
        checker = Dependency()
        event = {"dag_id": "first_dag",
                 "execution_date": datetime(2023, 4, 22),
                 "waiting_time": 4,
                 "max_waiting_count": 2,
                 "base_url": "http://43.143.250.12:8080"
                 }
        with self.assertRaises(ValueError):
            checker.check_dependencies(event)

    # 检查DAG已完成
    def test_check_dag_status_success(self):
        checker = Dependency()
        event = {"dag_id": "first_dag",
                 "execution_date": datetime(2023, 4, 23),
                 "waiting_time": 4,
                 "max_waiting_count": 2,
                 "base_url": "http://43.143.250.12:8080"
                 }
        self.assertEqual(checker.check_dependencies(event), 'success')

    # 检查无DAG运行记录
    def test_check_dag_status_no_dag_run(self):
        checker = Dependency()
        event = {"dag_id": "first_dag",
                 "execution_date": datetime(2023, 4, 30),
                 "waiting_time": 4,
                 "max_waiting_count": 2,
                 "base_url": "http://43.143.250.12:8080"
                 }
        with self.assertRaises(ValueError):
            checker.check_dependencies(event)
