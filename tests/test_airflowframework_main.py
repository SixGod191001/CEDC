import unittest
from airflow_framework.workspace import main


class TestAirflowFrameworkMain(unittest.TestCase):
    def test_Main(self):
        self.assertEqual(None, main.unit_test_main())  # add assertion here
