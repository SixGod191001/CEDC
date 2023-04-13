# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/14 1:18
"""
import os
import unittest
from unittestreport import TestRunner


def run_unittest():
    """
        :param suites: 测试套件
        :param filename: 报告文件名
        :param report_dir:报告文件的路径
        :param title:测试套件标题
        :param templates: 可以通过参数值1或者2，指定报告的样式模板，目前只有两个模板
        :param tester:测试者
    """
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
