# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/3/9 22:23
"""

import abc
import random
from datetime import datetime




class TargetInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def write_dynamic_frame(self):
        pass


class S3CsvTarget(TargetInterface):
    def __init__(self, pre_node=None, database=None, table_name=None, need_single_file=True, bucket_url=None):
        """
        :param database: glue database name
        :param table_name: table name in glue
        :return:
        """
        self.pre_node = pre_node
        self.need_single_file = need_single_file
        self.bucket_url = bucket_url
        self.database = database
        self.table_name = table_name
        self.transformation_ctx = "{table_name}_node{random_id}".format(table_name=table_name,
                                                                        random_id=random.randint(1000000000001,
                                                                                                 1999999999999))

    def write_dynamic_frame(self):
        # s3 按照 年 > 月 > 日的文件夹分区结构存储目标文件
        d1 = datetime.today()
        sub_path = str(d1.year) + '/' + str(d1.month) + '/' + str(d1.day) + '/'

        result_str = "# Script generated for node {NodeName}\n".format(NodeName=self.table_name)

        if self.need_single_file:
            single_file_str = '''repartition_frame = {PreNode}.repartition(1)\n'''.format(PreNode=self.pre_node)
            self.pre_node = 'repartition_frame'
            result_str = result_str + single_file_str

        write_df_str: str = '''{transformation_ctx} = glueContext.write_dynamic_frame.from_options(
            frame={PreNode},
            connection_type="s3",
            format="csv",
            connection_options = {{
                "path": "{bucket_url}",
                "partitionKeys": [],
            }},
            transformation_ctx="{transformation_ctx}",
        )'''.format(transformation_ctx=self.transformation_ctx, PreNode=self.pre_node, bucket_url=self.bucket_url+sub_path)
        result_str = result_str + write_df_str
        # print(result_str)
        return self.transformation_ctx, result_str


if __name__ == "__main__":
    s3t = S3CsvTarget(pre_node='S3 bucket', database='', table_name='S3bucket',
                      bucket_url='s3://lvdian-cedc-bucket/sales-target-data/')
    re1, re2 = s3t.write_dynamic_frame()
    print(re1)
    print(re2)


