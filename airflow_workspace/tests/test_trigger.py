import unittest

from airflow_framework.workspace.module.trigger import Trigger

class TestTrigger(unittest.TestCase):
    # 检查DAG执行
    def test_get_trigger(self):
        event = {"dag_run_id": "dhjglgtifju"}

        self.assertTrue(Trigger().trigger_dag(event))
