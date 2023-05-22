import unittest
from airflow_workspace.utils import email_handler


class Testemail_handler(unittest.TestCase):

    def testsend_email_ses(self):
        check = email_handler.EmailHandler()
        self.assertTrue(check.send_email_ses)

    def testsend_email_sns(self):
        check = email_handler.EmailHandler()
        self.assertTrue(check.send_email_sns)

if __name__ == "__main__":
    subject = "SEND EMAIL"
    body_test = ("EMAIL SEND")
    self = ' '
    Testemail_handler.testsend_email_sns(self=self,subject=subject,body_test=body_test)
    Testemail_handler.testsend_email_ses(self=self,subject=subject,body_test=body_test)
