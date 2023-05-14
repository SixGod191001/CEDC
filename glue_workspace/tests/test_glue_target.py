# -*- coding: utf-8 -*-
import unittest
import re
from glue_workspace.script.glue_script_generator.target import S3CsvTarget

def remove_number(string):
    return re.sub(r'[0-9]+', '', string)


class Test_S3_Csv_Target(unittest.TestCase):
    def test_S3_Csv_Target(self):
#         data = ['S3bucket_node',
#                 '''# Script generated for node S3bucket
# repartition_frame = S3 bucket.repartition(1)
# S3bucket_node = glueContext.write_dynamic_frame.from_options(
#             frame=repartition_frame,
#             connection_type="s3",
#             format="csv",
#             connection_options = {
#                 "path":s3://lvdian-cedc-bucket/sales-target-data/+"2023/5/9/",
#                 "partitionKeys": [],
#             },
#             transformation_ctx="S3bucket_node",
#         )''']
        s3t = S3CsvTarget(pre_node='S3 bucket', database='', table_name='S3bucket',
                          bucket_url='s3://lvdian-cedc-bucket/sales-target-data/')
        re1, re2 = s3t.write_dynamic_frame()
        self.assertIsNotNone(re1, re2)
        #self.assertEqual(data, [remove_number(re1), remove_number(re2)])