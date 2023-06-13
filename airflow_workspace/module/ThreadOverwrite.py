# -*- coding: utf-8 -*-
"""
@Author : Logan Xie
@Time : 2023/6/7 13:57
"""

import threading
import time


class MyThread(threading.Thread):
    """
    重写Thread 使其接受返回值
    """
    def __init__(self, func, args=()):
        super(MyThread, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        time.sleep(2)
        self.result = self.func(*self.args)
        # print(self.result)

    def get_result(self):
        threading.Thread.join(self)  # 等待线程执行完毕
        try:
            return self.result
        except Exception:
            return None
