import unittest
from airflow_workspace.module import start

class TestStart(unittest.TestCase):
    def test_start(self):
        event = {"datasource_name": "sample",
                 "load_type": "ALL",
                 "run_type": "glue",
                 "glue_template_name": "devops.prelanding.s3_file_movement"}
        self.assertEqual(None, start.Start().run(event))  # add assertion here
