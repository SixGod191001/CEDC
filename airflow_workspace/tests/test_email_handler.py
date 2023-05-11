import unittest
from airflow_workspace.utils import email_handler


class Testemail_handler(unittest.TestCase):
    def testsend_email_ses(self):
        check = email_handler.EmailHandler()
        self.assertTrue(check.send_email_ses)
    def testsend_email_sns(self):
        check = email_handler.EmailHandler()
        self.assertTrue(check.send_email_sns)