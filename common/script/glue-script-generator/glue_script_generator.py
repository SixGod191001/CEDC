# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/3/11 20:42
"""

"""
程序的主调用入口
接收两个参数 database=glue data catalog中数据库的名称，sql_path, 需要转换成脚本的sql文件路径
程序主体内容：调用有关接口生成代码段，最后拼接生成完整的glue脚本，并保存到script文件夹
返回值：glue脚本生成的路径
"""
