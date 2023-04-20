# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/16 1:07
@Desc : 记录airflow framework程序执行的log, by user level (user=调用者)
        Log文件放在S3上且文件命名规范如下：
         airflow/log/<dag_name>/YYYY/MM/DD/<dag_name>_YYYYMMDDHHMMSS.log
"""
from airflow_framework.workspace.utils.aws_handler import S3Handler


class Logger:
    def __init__(self):
        pass
