# -*- coding: utf-8 -*-

"""
@Author : YANG YANG
@Date : 2023/4/16 1:33
"""
from airflow.exceptions import AirflowException  # failed with retry
from airflow.exceptions import AirflowFailException  # failed without retry
import logger_handler

logger = logger_handler.logger()


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
            logger.info(f"执行[{func.__name__}]失败, args:{args}, kwargs:{kwargs} 异常:{e}")
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
            logger.info(f"执行[{func.__name__}]失败, args:{args}, kwargs:{kwargs} 异常:{e}")
            raise AirflowFailException

    return warp
