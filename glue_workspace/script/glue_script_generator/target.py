# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/3/9 22:23
"""

import abc
import random
# from datetime import datetime

class TargetInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def write_dynamic_frame(self):
        pass


class S3CsvTarget(TargetInterface):
    def __init__(self, pre_node=None, database=None, table_name=None, need_single_file=True, bucket_url=None, partition_counts=None):
        """
        :param database: glue database name
        :param table_name: table name in glue
        :param pre_node : DynamicFrame name to be unload to s3
        :param need_single_file: boolean , ture for one file
        :param bucket_url: full path for s3 url
        :return: 1,DynamicFrame name   2,glue script code in String
        """
        self.pre_node = pre_node
        self.need_single_file = need_single_file
        self.bucket_url = bucket_url
        self.partition_counts = partition_counts
        self.database = database
        self.table_name = table_name
        
        self.transformation_ctx = "{table_name}_node{random_id}".format(table_name=table_name,
                                                                        random_id=random.randint(1000000000001,
                                                                                                 1999999999999))

    def write_dynamic_frame(self):
        # s3 按照 年 > 月 > 日的文件夹分区结构存储目标文件
        # d1 = datetime.today()
        # sub_path = str(d1.year) + '/' + str(d1.month) + '/' + str(d1.day) + '/'

        result_str = "# Script generated for node {NodeName}\n".format(NodeName=self.table_name)
       # conn = PostgresHandler()
        if self.need_single_file:
            single_file_str = '''repartition_frame = {PreNode}.repartition({{partition_counts}})\n'''.format(PreNode=self.pre_node)
            self.pre_node = 'repartition_frame'
            result_str = result_str + single_file_str

        write_df_str: str = '''{transformation_ctx} = glueContext.write_dynamic_frame.from_options(
            frame={PreNode},
            connection_type="s3",
            format="csv",
            connection_options = {{
                "path": {bucket_url},
                "partitionKeys": [],
            }},
            transformation_ctx="{transformation_ctx}",
        )'''.format(transformation_ctx=self.transformation_ctx, PreNode=self.pre_node, bucket_url=self.bucket_url)
        result_str = result_str + write_df_str
        # print(result_str)
        return self.transformation_ctx, result_str

class PostgreSQLTarget(TargetInterface):
    def __init__(self, pre_node=None):
        self.pre_node = pre_node
        # self.database = database
        # self.table_name = table_name
        
        self.transformation_ctx = "PostgreSQL_node{random_id}".format(random_id=random.randint(1000000000001,
                                                                                                 1999999999999))
    def write_dynamic_frame(self):
        result_str = "# Script generated for node {table_name}\n"

        write_df_str: str = r'''{transformation_ctx} = glueContext.write_dynamic_frame.from_catalog(
            frame={PreNode},
            database="{{database}}",
            table_name="{{table_name}}",
            transformation_ctx="{transformation_ctx}",
        )'''.format(transformation_ctx=self.transformation_ctx, PreNode=self.pre_node)

        result_str = result_str + write_df_str

        return self.transformation_ctx, result_str

    
class MySQLTarget(TargetInterface):
    def __init__(self, pre_node=None):
        self.pre_node = pre_node
        # self.database = database
        # self.table_name = table_name
        
        self.transformation_ctx = "MySQL_node{random_id}".format(random_id=random.randint(1000000000001,
                                                                                                 1999999999999))
    def write_dynamic_frame(self):
        result_str = "# Script generated for node {table_name}\n"
        
        write_df_str: str = '''{transformation_ctx} = glueContext.write_dynamic_frame.from_catalog(
            frame={PreNode},
            database="{{database}}",
            table_name="{{table_name}}",
            transformation_ctx="{transformation_ctx}",
        )'''.format(transformation_ctx=self.transformation_ctx, PreNode=self.pre_node)
        result_str = result_str + write_df_str

        return self.transformation_ctx, result_str

if __name__ == "__main__":
    # s3t = S3CsvTarget(pre_node='S3 bucket', database='', table_name='S3bucket',
    #                   bucket_url='target_path', partition_counts=2)
    # re1, re2 = s3t.write_dynamic_frame()
    # print(re1)
    # print(re2)

    # 测试PostgreSQLTarget
    pgt = PostgreSQLTarget(pre_node='PostgreSQL')
    re1, re2 = pgt.write_dynamic_frame()
    print(re1)
    print(re2)




