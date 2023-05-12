import boto3
from airflow.exceptions import AirflowException  # failed with retry
from botocore.exceptions import ClientError
import logging

from airflow_workspace.utils import boto3_client

logger = logging.getLogger(__name__)
# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/16 1:30
"""


class EmailHandler:

    def __init__(self):
        pass

    def send_email_ses(self, subject, body_text):
        """
        通过aws ses 发送邮件
        :param recipient: 收件人邮箱
        :param subject: 标题
        :param body_text: 正文
        :return: True/False
        """

        # Replace sender@example.com with your "From" address.
        # This address must be verified with Amazon SES.
        SENDER = "958144600@qq.com"
        recipient = "wuyanbing3@live.com"

        # The character encoding for the email.
        CHARSET = "UTF-8"


        # Try to send the email.
        try:
            # Create a new SES resource and specify a region.
            client = boto3_client.get_aws_boto3_client(service_name='ses')
            # Provide the contents of the email.
            response = client.send_email(
                Destination={
                    'ToAddresses': [
                        recipient,
                    ],
                },
                Message={
                    'Body': {
                        # 'Html': {
                        #     'Charset': CHARSET,
                        #     'Data': BODY_HTML,
                        # },
                        'Text': {
                            'Charset': CHARSET,
                            'Data': body_text,
                        },
                    },
                    'Subject': {
                        'Charset': CHARSET,
                        'Data': subject,
                    },
                },
                Source=SENDER,
                # If you are not using a configuration set, comment or delete the
                # following line
                # ConfigurationSetName=CONFIGURATION_SET,
            )
        # Display an error if something goes wrong.
        except ClientError as e:
            logger.exception(
                "Couldn't send email : '%s'", e.response['Error']['Message'])
            raise AirflowException("send email fail.")
        else:
            logger.info("Email sent! Message ID: '%s'", response['MessageId'])
            return True
        return False

    def send_email_sns(self, subject, body_text):
        """
        通过aws sns 发送邮件
        :param subject: 标题
        :param body_test: 正文
        :return: True/False
        """
        # Create a new SES resource and specify a region.
        sns_client = boto3_client.get_aws_boto3_client(service_name='sns')

        MY_SNS_TOPIC_ARN = 'arn:aws:sns:ap-northeast-1:021255973451:email'
        try:
            response = sns_client.publish(
                TopicArn=MY_SNS_TOPIC_ARN,
                Message=body_text,
                Subject=subject
            )
            logger.info(response)
        except ClientError as e:
            logger.exception(
                "Couldn't send email : '%s'", e.response['Error']['Message'])
            raise AirflowException("send email fail.")
        else:
            logger.info("Email sent! Message ID: '%s'", response['MessageId'])
            return True
        return False