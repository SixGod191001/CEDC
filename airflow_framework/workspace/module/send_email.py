from airflow_framework.workspace.utils.email_handler import EmailHandler


class SendEmail:
    def __init__(self):
        pass

    def send_job_result(self, status, job_name):
        """
                dag执行结果邮件通知
                :param status:  Succeed/Failed
                :param job_name:
                :return: True/False
                """
        recipient = "wuyanbing3@live.com"
        subject = ""
        body_test = ""
        if status == "Succeed":
            subject = job_name + "执行成功"
            body_test = job_name + "执行成功"
        else:
            subject = job_name + "执行失败"
            body_test = job_name + "执行失败，请检查"
        return EmailHandler().send_email_ses(recipient, subject, body_test)






if __name__ == "__main__":
    SendEmail().send_job_result("Failed ", "job1234")

