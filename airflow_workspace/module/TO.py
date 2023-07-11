# -*- coding: utf-8 -*-
"""
@Author : Logan Xie
@Time : 2023/6/12 17:34
"""
import threading

def my_func():
    # 执行一些操作
    result = 42  # 假设这是函数的返回值
    return result

my_thread = threading.Thread(target=my_func)
my_thread.start()
my_thread.join()

# 获取返回值
result = my_thread.result