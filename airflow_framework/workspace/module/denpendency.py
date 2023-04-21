# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/16 1:31
"""
import time
from datetime import datetime, timedelta

jobs = [
    {"job_name": "Job-1", "run_date": "2023-04-01 10:00:00", "status": "SUCCEEDED"},
    {"job_name": "Job-2", "run_date": "2023-04-01 12:00:00", "status": "FAILED"},
    {"job_name": "Job-3", "run_date": "2023-04-02 09:00:00", "status": "TIMEOUT"},
    {"job_name": "Job-4", "run_date": "2023-04-02 11:00:00", "status": "WAITING"},
    {"job_name": "Job-5", "run_date": "2023-04-03 08:00:00", "status": "STOPPING"},
    {"job_name": "Job-6", "run_date": "2023-04-03 10:00:00", "status": "RUNNING"},
    {"job_name": "Job-7", "run_date": "2023-04-04 11:00:00", "status": "STARTING"},
    {"job_name": "Job-8", "run_date": "2023-04-04 13:00:00", "status": "ERROR"},
    {"job_name": "Job-9", "run_date": "2023-04-05 10:00:00", "status": "notfound"},
    {"job_name": "Job-10", "run_date": "2023-04-05 12:00:00", "status": "RUNNING"},
]

class Dependency:
    def __init__(self, job_name, run_date, dependency_names=None, waiting_time=60):
        self.job_name = job_name
        self.run_date = run_date
        self.waiting_time = waiting_time
        self.dependency_names = dependency_names if dependency_names else []

    def get_dependency_status(self, job_name, run_date):
        for job in jobs:
            if job["job_name"] == job_name and job["run_date"] == run_date:
                return job["status"]
        return None

    def check_dependencies(self):
        count = 0
        while True:
            dependency_statuses = [self.get_dependency_status(name, self.run_date) for name in self.dependency_names]
            if None in dependency_statuses:
                raise ValueError('依赖任务未找到: ' + str(self.dependency_names))
            if 'notfound' in dependency_statuses:
                raise ValueError('依赖任务未找到: ' + str([name for name, status in zip(self.dependency_names, dependency_statuses) if status == 'notfound']))
            if all(status == 'SUCCEEDED' for status in dependency_statuses):
                return True
            elif any(status in {'STARTING', 'RUNNING', 'STOPPING', 'WAITING'} for status in dependency_statuses):
                count += 1
                if count >= 3:
                    raise ValueError('依赖任务等待时间过长: ' + str(self.dependency_names))
            else:
                return False
            time.sleep(self.waiting_time)


