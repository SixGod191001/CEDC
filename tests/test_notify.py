import unittest

from airflow_framework.workspace.module.notify import Notify


class TestSource(unittest.TestCase):
    def test_send_job_result(self):
        event = {"datasource_name": "sample",
                 "load_type": "ALL",
                 "run_type": "glue",
                 "glue_template_name": "devops.prelanding.s3_file_movement"}
        self.assertTrue(Notify().send_job_result(event))
