# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/14 1:18
"""
import os
import unittest
from unittestreport import TestRunner


def run_unittest():
    base_path = os.path.abspath(os.path.dirname(__file__))
    cases = unittest.defaultTestLoader.discover("./")

    # 执行用例

    runner = TestRunner(cases,
                        filename="unittest.html",
                        report_dir=os.path.join(base_path, 'report'),
                        title="单元测试报告",
                        tester="YANG YANG",
                        desc="单元测试报告",
                        templates=1
                        )
    # 2.运行套件
    runner.run()


if __name__ == '__main__':
    run_unittest()
