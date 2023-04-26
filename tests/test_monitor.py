# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/13 23:08
"""
import unittest
import argparse
import json
from airflow_framework.workspace.module import monitor


class TestMonitor(unittest.TestCase):
    def test_monitor_SUCCEEDED(self):
        parser = argparse.ArgumentParser(description='Get variables from task in Airflow DAG')
        parser.add_argument("--params", type=str,
                            default='{"datasource_name": "sample", "load_type": "ALL", "run_type": "glue", '
                                    '"glue_template_name":"cedc_sales_prelanding_template"}')

        args = parser.parse_args()
        batch_event = json.loads(args.params)
        m = monitor.Monitor(batch_event)
        self.assertIsNone(m.monitor())
