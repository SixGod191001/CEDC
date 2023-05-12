# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/16 1:29
"""
from airflow_workspace.utils.constants import Constants
from airflow_workspace.utils.email_handler import EmailHandler
from airflow_workspace.utils.exception_handler import catch_fail_exception
from airflow_workspace.utils.postgre_handler import PostgresHandler
from airflow_workspace.utils.logger_handler import logger

logger = logger()


class Notify:
    @catch_fail_exception
    def send_job_result(self, event):
        """
                dag执行结果邮件通知
                :param event:
                :return: True/False
                """

        job_name = event['datasource_name']
        ph = PostgresHandler()
        sqlStr = Constants.SQL_GET_JOB_STATE
        status = ph.get_record(sqlStr.format(job_name=job_name))
        if not status:
            logger.error("没有找到job '%s'", job_name)
            return
        else:
            job_status = status[0][0]
            ph = PostgresHandler()
            sql_email = Constants.SQL_GET_EMAIL
            email = ph.get_record(sql_email.format(topic='notify', email_type=job_status))
            if not email:
                logger.error("邮件发送失败，没有找到邮件模板")
                return
            else:
                if job_status == "success" or job_status == "failed":
                    subject = email[0][0]
                    body_text = email[0][1].format(job_name=job_name)
                    return EmailHandler().send_email_ses(subject, body_text)
                else:
                    logger.error("无效状态： '%s' ", job_status)
                    return


# if __name__ == "__main__":
#     event = {"datasource_name": "cedc_sales_prelanding_job2",
#              "load_type": "ALL",
#              "run_type": "glue",
#              "glue_template_name": "devops.prelanding.s3_file_movement"}
#     Notify().send_job_result(event)
