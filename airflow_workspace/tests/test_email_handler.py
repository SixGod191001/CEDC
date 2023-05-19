import unittest
from airflow_workspace.utils import email_handler


class Testemail_handler(unittest.TestCase):

    def testsend_email_ses(self):
        check = email_handler.EmailHandler()
        self.assertIsInstance(check.send_email_ses,bool)

    def testsend_email_sns(self):
        check = email_handler.EmailHandler()
        self.assertIsInstance(check.send_email_sns,bool)

if __name__ == "__main__":
    Testemail_handler.testsend_email_sns()
    Testemail_handler.testsend_email_ses()
