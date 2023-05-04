# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/3/9 22:23
"""

# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/3/9 22:23
"""
import abc
import re
from glue_workspace.script.utils import nodetool, filetool


# 生成 Glue Transform代码

class TransformInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def transform(self, sql_query):
        pass

    @abc.abstractmethod
    def sql_query(self):
        pass


class TransformGenerator(TransformInterface):
    def __init__(self, sql_path=None, datasource_node=()):
        """
        :param sql_path:
        :param datasource_node: tuple 参数传入不定个数的datasource node
        """
        nt = nodetool.NodeTool()
        self.sql_path = sql_path
        self.datasource_node = datasource_node
        self.transformation_ctx = nt.get_ctx_name('SQLTransform')

    def transform(self):
        """
        :param
        :return:
        """
        headstr = '# Script generated for node SQL Query \nSqlQuery0 = """'
        transform_node, dynamic_frame_str = self.create_dynamic_frame()
        transformstr = headstr + '\n' + self.sql_query() + '\n' + '"""' + '\n' + dynamic_frame_str
        return transform_node, transformstr

    def sql_query(self):
        """
        从sql文件读取sql内容，并返回sql内容，要求格式不丢失
        :return:
        """
        ft = filetool.FileTool(self.sql_path)
        sqlstr = ft.read_file()
        pattern = r";\s*$"
        if re.search(pattern, sqlstr):
            sqlstr = re.sub(pattern, '', sqlstr)
        return sqlstr

    def create_dynamic_frame(self):
        mapping_str = ''
        nt = nodetool.NodeTool()
        for node in self.datasource_node:
            node_name = nt.get_node_name(node)
            table_name = nt.get_node_property_value(node, 'table_name')
            mapping_str += '\t"{tablename}":{tablenode},\n\t\t'.format(tablename=table_name, tablenode=node_name)

        result_str = '''{0} = sparkSqlQuery(
        glueContext,
        query=SqlQuery0,
        mapping={{
        {1}
        }},
        transformation_ctx="{0}",
        )'''.format(self.transformation_ctx, mapping_str)
        return self.transformation_ctx, result_str

# if __name__ == '__main__':
#     u = 'https://bkt-dfk.s3.ap-northeast-1.amazonaws.com/workspace/forremote/tmp/test.sql?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjELH%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaDmFwLXNvdXRoZWFzdC0xIkgwRgIhAOxjVqurmSI2mA1BT26m4KMNyMqYSgOVkkWWaI%2FoPvl6AiEA4zZKuIPX8zaaaO%2F0C4sBMcF%2FJgsfBvMi4LRpOHzrfHcqjgMImv%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAAGgwwNjQwNTU1NDUwMTgiDDf4W0ol38dpNF4MESriAsAD4quC%2FejQLSqqZufEINhFm5FKMRv531ldp3TYZYuhMQOHsscrOi%2FkH2uJerDdjZX44ifMcHcEMs9tAPapiF8o6idt8qkwbcxEKIOEvtcPx8N%2FTjQfgOpHHfHH%2FlAUdNQ9Jrg8ONM4%2F%2FX%2BrSQQAMhSHdjcqopLbYIKWIRRuvjsskWuJh%2FE7Ud8jn4qDwM3%2BfclN2I5HrdKvRPXuwNE7Y1LZze5EcoTFzHm7LYauvKGTQKR8xS3snnWmYAk95c%2F2gSeqRJVmCLJSv1JbsXHRdiqpEyuEeqTJrhleOcBvaBoLWQmvMN6is65CUxkNFHMhEk2nfBgUVNbAKqw6jD1ypuDy7y4x755A%2F76aLnaP0q8Wrgzs1v9No7WtMbDFF7dtjP0T%2Fu0aHpzUwwrwBcpQCBDTtcg9ZhvEdin6VXskIEZGM3c2DXJ0KJ7I%2BHRFhZqHy0ePE86sqEL0sxjI%2BM%2F4wuCtDDT3dKhBjqyAhKoIAgPLvgIH6Oex1oTeWE5bbd1w%2FCRHHNeoC56Miv5mHssfQdwgateEa%2BWYZaeYy6m4Cj5liwhN4bgHhu%2FfUTd3XR36xMqzM7zMuDxj%2BGBx8EU5YpkwlJWvmFMQOiLuG2Juwhj88w1LF8v6aYf2GZhsZpIGrC0A1DWOf8KI02VUfhe8%2B7VE3KNlD0aAkhCBzuXfi%2Bx5aV5O7Icpi0QxlwIhROJ5G5ddmckYNSbfW8jjpka3uLlWyc0FleegRA7eYT7TGWCr42G9XOl1v5%2FgcToxg09HhtM8pWBXUDknQqefw8BkoBGoWG0eeGISrYyA8cn99ZyYn%2Bai%2BoyEnHOuZ0au%2Bnh0puBGEW4Rvobb1%2BZ7XYVsDpAe3XeqOCfmwI6ERUXuPOcMOOHDGK1oANSYnfNew%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230411T011336Z&X-Amz-SignedHeaders=host&X-Amz-Expires=32400&X-Amz-Credential=ASIAQ52QCBS5NXWIFPJW%2F20230411%2Fap-northeast-1%2Fs3%2Faws4_request&X-Amz-Signature=35ce7aa8e6c8ab461a470fa9e4877809dd841d5d5af94479bc7980ed61008d72'
#     tg=TransformGenerator(u, (['# Script generated for node employees\nemployees_node1948554591402 = glueContext.create_dynamic_frame.from_catalog(\n    database="devops",\n    table_name="employees",\n    transformation_ctx="employees_node1948554591402",\n)', '# Script generated for node departments\ndepartments_node1406226729501 = glueContext.create_dynamic_frame.from_catalog(\n    database="devops",\n    table_name="departments",\n    transformation_ctx="departments_node1406226729501",\n)']))
#     #print(tg.create_dynamic_frame())
#     #print(tg.sql_query())
#     print("==========")
#     #print(tg.sql_query())
#     #a=tg.sql_query()
#     print(tg.transform())
