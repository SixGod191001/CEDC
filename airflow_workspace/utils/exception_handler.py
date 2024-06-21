# -*- coding: utf-8 -*-

"""
@Author : YANG YANG
@Date : 2023/4/16 1:33
"""
from airflow.exceptions import AirflowException  # failed with retry
from airflow.exceptions import AirflowFailException  # failed without retry
from airflow_workspace.utils.logger_handler import logger

logger = logger()


def catch_exception(func):
    """
    失败重试
    :param func:
    :return:
    """

    def warp(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Execute [{func.__name__}] failed, args:{args}, kwargs:{kwargs} . And exception is:{e}")
            raise AirflowException

    return warp


def catch_fail_exception(func):
    """
    失败不重试
    :param func:
    :return:
    """

    def warp(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Execute [{func.__name__}] failed, args:{args}, kwargs:{kwargs}. And exception is:{e}")
            raise AirflowFailException

    return warp
