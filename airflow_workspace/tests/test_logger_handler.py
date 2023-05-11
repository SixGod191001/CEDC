import unittest
import logging
from airflow_workspace.utils import logger_handler


class TestLogger(unittest.TestCase):
    def test_logger(self):
        logger = logger_handler.logger()
        self.assertIsNone(logger.info("This is test"))

if __name__ == "__main__":
    TestLogger.test_logger()