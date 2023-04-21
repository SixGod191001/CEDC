import unittest

from unittest.mock import patch
from airflow_framework.workspace.module.denpendency import Dependency, jobs

class TestDependency(unittest.TestCase):
    
    def test_get_dependency_status(self):
        # 测试能够正确获取依赖任务的状态
        dep = Dependency('Job-1', '2023-04-01 10:00:00')
        status = dep.get_dependency_status('Job-1', '2023-04-01 10:00:00')
        self.assertEqual(status, 'SUCCEEDED')
        
        status = dep.get_dependency_status('Job-2', '2023-04-01 12:00:00')
        self.assertEqual(status, 'FAILED')
        
        status = dep.get_dependency_status('Job-5', '2023-04-03 08:00:00')
        self.assertEqual(status, 'STOPPING')
        
        status = dep.get_dependency_status('Job-9', '2023-04-05 10:00:00')
        self.assertEqual(status, 'notfound')
        
        status = dep.get_dependency_status('Job-11', '2023-04-05 12:00:00')
        self.assertEqual(status, None)
        
    @patch('dependency.time.sleep', return_value=None)
    def test_check_dependencies(self, mock_sleep):
        # 测试依赖任务状态为SUCCEEDED时，check_dependencies方法能够返回True
        dep = Dependency('Job-1', '2023-04-01 10:00:00')
        result = dep.check_dependencies()
        self.assertTrue(result)
        
        # 测试依赖任务状态为WAITING时，check_dependencies方法能够等待一定时间后抛出异常
        dep = Dependency('Job-4', '2023-04-02 11:00:00')
        with self.assertRaises(ValueError):
            dep.check_dependencies()
            
        # 测试依赖任务状态为STARTING时，check_dependencies方法能够等待一定时间后抛出异常
        dep = Dependency('Job-7', '2023-04-04 11:00:00')
        with self.assertRaises(ValueError):
            dep.check_dependencies()
            
        # 测试依赖任务状态为RUNNING时，check_dependencies方法能够等待一定时间后抛出异常
        dep = Dependency('Job-6', '2023-04-03 10:00:00')
        with self.assertRaises(ValueError):
            dep.check_dependencies()
            
        # 测试依赖任务状态为TIMEOUT时，check_dependencies方法能够返回False
        dep = Dependency('Job-3', '2023-04-02 09:00:00')
        result = dep.check_dependencies()
        self.assertFalse(result)
        
        # 测试依赖任务状态为ERROR时，check_dependencies方法能够返回False
        dep = Dependency('Job-8', '2023-04-04 13:00:00')
        result = dep.check_dependencies()
        self.assertFalse(result)
        
        # 测试依赖任务状态为notfound时，check_dependencies方法能够抛出异常
        dep = Dependency('Job-9', '2023-04-05 10:00:00')
        with self.assertRaises(ValueError):
            dep.check_dependencies()
