# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/16 1:29
"""
from airflow_workspace.utils.email_handler import EmailHandler


class Notify:
    def send_job_result(self, event):
        """
                dag执行结果邮件通知
                :param event:
                :return: True/False
                """

        job_name = event['datasource_name']
        status = 'Failed'
        if status == "Succeed":
            subject = job_name + "执行成功"
            body_test = job_name + "执行成功"
        else:
            subject = job_name + "执行失败"
            body_test = job_name + "执行失败，请检查"
        return EmailHandler().send_email_ses(subject, body_test)
