# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/16 1:07
"""
import logging
import os
import sys
from airflow.exceptions import AirflowFailException  # make the task failed without retry
from airflow.exceptions import AirflowException  # failed with retry


def logger():
    """
    log 文件会记录调用者是谁  调用的函数 行号等
    :return:
    """
    try:
        back_frame = sys._getframe().f_back
        back_filename = os.path.basename(back_frame.f_code.co_filename)
        back_func_name = back_frame.f_code.co_name
        back_lineno = back_frame.f_lineno
        # define logger
        ret_logger = logging.getLogger(
            'frame: {} - file: {} - function:{} - line:{}'.format(back_frame, back_filename, back_func_name,
                                                                  back_lineno))
        ret_logger.setLevel(logging.INFO)
        # define stream output
        rf_handler = logging.StreamHandler(sys.stderr)  # 默认是sys.stderr
        rf_handler.setLevel(logging.DEBUG)
        rf_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(message)s"))
        ret_logger.addHandler(rf_handler)
        return ret_logger
    except Exception as e:
        raise AirflowFailException("Logger handler is bad!")


# Example: How to use it
# import logger_handler
# logger = logger_handler.logger()
# logger.info('this is demo')

if __name__ == "__main__":
    logger = logger()
    logger.info('this is logger handler')
