# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/16 0:28
@Desc : Airflow 框架的入口
"""
import argparse


def switch():
    """
    该方法用于接收不同的调用类型参数，然后调用对应的方法功能
    :return:
    """
    pass


if __name__ == "__main__":
    """
    调用示例： python main.py --job-name=sample --method=start --load-type=ALL
    job-name: glue job的名字，该名字时唯一的
    method: 调用的需要的方法，比如执行,发邮件 还是监控等
    load-type: Full Load (ALL) 或者是 Incremental Load（INC）
    :return:
    """
    parser = argparse.ArgumentParser(description='Get variables from task in Airflow DAG')
    parser.add_argument("--job-name", type=str, default="sample")
    parser.add_argument("--method", type=str, default='start')
    parser.add_argument("--load-type", type=str, default='ALL')
    args = parser.parse_args()
    print(args.job_name)
    print(args.method)
    print(args.load_type)
