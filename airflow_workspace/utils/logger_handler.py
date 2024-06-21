# -*- coding: utf-8 -*-
"""
@Author: YANG YANG
@Date: 2023/4/16 1:07
"""

import logging
import os
import sys
from airflow.exceptions import AirflowFailException, AirflowException


def get_logger():
    """
    Create and configure a logger that records the caller's information such as file name, function name, and line number.

    Returns:
    - logging.Logger: Configured logger instance.

    Raises:
    - AirflowFailException: If there is an error configuring the logger.
    """
    try:
        back_frame = sys._getframe().f_back
        back_filename = os.path.basename(back_frame.f_code.co_filename)
        back_func_name = back_frame.f_code.co_name
        back_lineno = back_frame.f_lineno

        # Define logger with caller's information
        ret_logger = logging.getLogger(
            'file: {back_filename} - function: {back_func_name} - line: {back_lineno}'.format(back_filename=back_filename, back_func_name=back_func_name,
                                                                                              back_lineno=back_lineno)
        )
        ret_logger.setLevel(logging.INFO)

        # Define stream handler
        stream_handler = logging.StreamHandler(sys.stderr)
        stream_handler.setLevel(logging.DEBUG)
        stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

        # Add handler to the logger
        ret_logger.addHandler(stream_handler)

        return ret_logger
    except Exception as e:
        error_message = "Failed to configure logger: {}".format(str(e))
        raise AirflowFailException(error_message)


if __name__ == "__main__":
    try:
        logger = get_logger()
        logger.info('This is a logger handler example.')
    except AirflowFailException as error:
        sys.stderr.write("Logger configuration error: {}\n".format(str(error)))
        sys.exit(1)
