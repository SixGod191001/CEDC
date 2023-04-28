import unittest

from airflow_workspace.module import Notify


class TestNotify(unittest.TestCase):
    def test_send_job_result_true(self):
        event = {"datasource_name": "sample",
                 "load_type": "ALL",
                 "run_type": "glue",
                 "glue_template_name": "devops.prelanding.s3_file_movement"}
        self.assertTrue(Notify().send_job_result(event))

    def test_send_job_result_flase(self):
        event = {"datasource_name": "sample",
                 "load_type": "ALL",
                 "run_type": "glue",
                 "glue_template_name": "devops.prelanding.s3_file_movement"}
        self.assertFalse((Notify().send_job_result(event)))
