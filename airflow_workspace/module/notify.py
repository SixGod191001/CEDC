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

        dag_name = event['datasource_name']
        ph = PostgresHandler()
        sqlStr = Constants.SQL_GET_DAG_STATE
        status = ph.get_record(sqlStr.format(dag_name=dag_name))
        if not status:
            logger.error("没有找到dag '%s'", dag_name)
            return
        else:
            dag_status = status[0]['status']
            ph = PostgresHandler()
            sql_email = Constants.SQL_GET_EMAIL
            email = ph.get_record(sql_email.format(topic='notify', email_type=dag_status))
            if not email:
                logger.error("邮件发送失败，没有找到邮件模板")
                return
            else:
                if dag_status == Constants.GLUE_SUCCEEDED or dag_status == Constants.GLUE_FAILED:
                    subject = email[0]['email_header']
                    body_text = email[0]['email_body'].format(dag_name=dag_name)
                    return EmailHandler().send_email_ses(subject, body_text)
                else:
                    logger.error("无效状态： '%s' ", dag_status)
                    return


# if __name__ == "__main__":
#     event = {"datasource_name": "dag_cedc_sales_prelanding",
#              "load_type": "ALL",
#              "run_type": "glue",
#              "glue_template_name": "devops.prelanding.s3_file_movement"}
#     Notify().send_job_result(event)
