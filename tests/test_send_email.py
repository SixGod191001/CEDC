import unittest

from airflow_framework.workspace.module.send_email import SendEmail


class TestSource(unittest.TestCase):
    def test_send_email_ses(self):
        SendEmail().send_job_result("Failed ", "job1234")