import unittest
from airflow import DAG
from airflow_workspace.utils import airflow_handler

class Test_airflow_handler(unittest.TestCase):

    def test_get_airflow_task_info(self):
        self.assertIsInstance(airflow_handler.get_airflow_task_info(),dict)

if __name__ == "__main__":
    Test_airflow_handler()