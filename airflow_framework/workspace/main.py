# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/16 0:28
@Desc : Airflow 框架的入口
"""
import argparse
import json
from module.start import Start


def dependency_check(event):
    print("这是假的， 需要从对应的文件引用，可以模仿Start类写，event is: {}".format(event))


def monitor_batch(event):
    print("这是假的， 需要从对应的文件引用，可以模仿Start类写，event is: {}".format(event))


def batch_notify(event):
    print("这是假的， 需要从对应的文件引用，可以模仿Start类写，event is: {}".format(event))


def trigger_next_dag(event):
    print("这是假的， 需要从对应的文件引用，可以模仿Start类写，event is: {}".format(event))


def check_trigger(trigger):
    """
    该方法接收airflow传入的不同类型的模块调用对应的方法
    :param trigger:dependency_check, start_batch, monitor_batch, batch_notify, trigger_next_dag
    :return:
    """
    switcher = {
        "dependency_check": dependency_check,
        "start_batch": Start().run,
        "monitor_batch": monitor_batch,
        "batch_notify": batch_notify,
        "trigger_next_dag": trigger_next_dag
    }
    # 返回值调用方法： switcher.get(choice, default)() # 执行对应的函数，如果没有就执行默认的函数,default为默认函数用lambda简化
    return switcher.get(trigger, lambda: "Invalid file type provided")


if __name__ == "__main__":
    """
    调用示例如下： 
    python main.py --trigger=start_batch --params='{"datasource_name": "sample", "load_type": "ALL", "run_type": "glue", "glue_template_name":"cedc_sales_prelanding_template"}'
    变量释义如下：
    trigger: 调用的需要的方法，比如执行,发邮件 还是监控等
    params: 类型：json字符串, 内含变量如下
            1) datasource_name：通过datasource_name可以在数据库中找到对应的batch_name, batch_id, 以及batch下需要执行的相关glue job
            2) load_type: Full Load (ALL) 或者是 Incremental Load（INC）
            3) run_type: glue 或者 python 或者 lambda 或者 存储过程 等等
            3) glue_template_name: 如果是触发的glue类型任务，需要指定使用哪个glue template
    :return:
    """
    # get parameters from airflow
    parser = argparse.ArgumentParser(description='Get variables from task in Airflow DAG')
    parser.add_argument("--trigger", type=str, default='start_batch')
    parser.add_argument("--params", type=str,
                        default='{"datasource_name": "sample", "load_type": "ALL", "run_type": "glue", '
                                '"glue_template_name":"cedc_sales_prelanding_template"}')
    args = parser.parse_args()

    # choose trigger module
    batch = check_trigger(args.trigger)

    # convert json string to dict
    batch_event = json.loads(args.params)

    # pass params into specific module
    batch(batch_event)

    # logger 方法需要抽出来 WIP
    # logger.info(batch(event, "context"))
