import unittest
from airflow_workspace.utils import exception_handler


"""
本地运行出现  no module logger_handler
需要 在 logger_handler 上 
from airflow_workspace import logger_handler
"""


class Test_exception_handler(unittest.TestCase):

    @exception_handler.catch_exception
    def test_catch_exception(*args, **kwargs):
        print(f"args是:{args}")
        print(f"kwargs是:{kwargs}")
        return "--"

    @exception_handler.catch_fail_exception
    def test_catch_fail_exception(*args, **kwargs):
        print(f"args是:{args}")
        print(f"kwargs是:{kwargs}")
        return "--"

if __name__ == '__main__':

    print(Test_exception_handler.test_catch_exception(1, 2, a=1, b=2))

    print(Test_exception_handler.test_catch_fail_exception(3, 4, a=3, b=4))