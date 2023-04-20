import boto3
from botocore.exceptions import ClientError

# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/16 1:30
"""


class EmailHandler:

    def __init__(self):
        pass

    def send_email_ses(self, recipient, subject, body_test):
        """
        通过aws ses 发送邮件
        :param recipient: 收件人邮箱
        :param subject: 标题
        :param body_test: 正文
        :return: True/False
        """

        # Replace sender@example.com with your "From" address.
        # This address must be verified with Amazon SES.
        SENDER = "958144600@qq.com"

        # If necessary, replace ap-northeast-1 with the AWS Region you're using for Amazon SES.
        AWS_REGION = "ap-northeast-1"

        # The character encoding for the email.
        CHARSET = "UTF-8"
        ACCESS_KEY = 'AKIAQJ4XUNJFYRLBDZYV'
        SECRET_KEY = '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'
        # Create a new SES resource and specify a region.
        client = boto3.client('ses', region_name=AWS_REGION, aws_access_key_id=ACCESS_KEY,
                              aws_secret_access_key=SECRET_KEY)

        # Try to send the email.
        try:
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
                            'Data': body_test,
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
            print(e.response['Error']['Message'])
        else:
            print("Email sent! Message ID:"),
            print(response['MessageId'])
            return True
        return False
