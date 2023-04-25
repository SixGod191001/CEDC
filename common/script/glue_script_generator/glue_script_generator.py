# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/3/11 20:42
"""
import source
import sql_query_parse
import target
import transform
import sys
from common.script.utils import strtool, constants, filetool

"""
程序的主调用入口
接收四个参数  database glue data catalog中数据库的名称
            sql_path 需要转换成脚本的sql文件路径（全路径带文件名）
            target_path target数据文件生成路径
            out_py_path 生成python文件的路径（全路径带文件名）
程序主体内容：调用有关接口生成代码段，最后拼接生成完整的glue脚本，并保存到script文件夹
返回值：glue脚本生成的路径
"""


class GlueScriptGenerate:
    def __init__(self, default_params =None):
        """
        :param database: glue data catalog中数据库的名称
        :param sql_path: 需要转换成脚本的sql文件路径
        :param target_path: target数据文件生成路径
        :param out_py_path: python文件的生成路径
        :return: glue脚本生成的路径
        """
        for k, v in default_params.items():
            setattr(self, k, v)
        # self.database = database
        # self.sql_path = sql_path
        # self.target_path = target_path
        # self.out_py_path = out_py_path

    def get_script(self):
        # 读取sql文件
        ft = filetool.FileTool(self.sql_path)
        sql = ft.read_url_file()
        # 解析sql，获取源表
        gt = sql_query_parse.GetTables(sql)
        tables = gt.get_element()

        '''    -----------------    拼接代码 Start    -----------------    '''
        # 获取head代码
        py_head_str = constants.Constants.PY_HEAD_STR

        # 获取source部分代码
        # source_ctx_lst = []
        source_node_part_lst = []
        py_source_str = ''
        st = strtool.StrTool()
        for table_nm in tables:
            source_ctx, source_node_part = source.generate_datasource_interface(
                source.CsvDatasource(database=self.database, table_name=table_nm))
            # source_ctx_lst.append(source_ctx)
            source_node_part_lst.append(source_node_part)
            py_source_str += st.add_enter_char(source_node_part)

        # 获取Transform部分代码
        tg = transform.TransformGenerator(self.sql_path, tuple(source_node_part_lst))
        transform_node, py_transform_str = tg.transform()

        # 获取Target部分代码
        s3t = target.S3CsvTarget(pre_node=transform_node, database=self.database,
                                 table_name='S3bucket', bucket_url=self.target_path)
        re1, py_target_str = s3t.write_dynamic_frame()

        # 获取tail代码
        py_tail_str = constants.Constants.PY_TAIL_STR

        # 拼接代码
        py_str = st.concate_strings_with_enter_char(py_head_str, py_source_str, py_transform_str, py_target_str,
                                                    py_tail_str)
        '''    -----------------    拼接代码 End    -----------------    '''

        # 输出py文件到对应目录
        ft = filetool.FileTool(self.out_py_path)
        ft.write_file(py_str)
        return self.out_py_path

if __name__ == '__main__':
    u = 'https://training2223333.s3.ap-northeast-1.amazonaws.com/sql/demo.sql?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjENb%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaDmFwLW5vcnRoZWFzdC0xIkYwRAIgSk62gZXMry6gTXi6hL%2Fd4TBBMqkmEqdwXwz4OLmmNo8CIBc%2B4IpRl29cRhtljrdR16yOXPLbqmVYYeHSCQEJeQadKoQDCM%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEQAhoMMjEzOTAzNTM0MzM3Igwu1WjLedP0ggCx6qkq2ALlrZeqBhFqHVY4SWyLabj5oHFrkSTr8uhNwJTsshUhlDP%2B5pHERhY5t3Ws2bFU4nNbvw3ZuqPnllEhh%2FfTlIjAypMw1PsB9H1Xcpmh9lphodvn1xAT9q5QxK1Gk%2BiNjtwToY%2BRANVW4qu1aHE9kfuwrrUSZHTh1NQo%2FahJpooSKAtAlEPBzgFf1u3iinpDVE5ieU42ERX2hBsIlcl4TNFC7jkJZcT7n8dJudE3mSqhE25wQR308Q3jF%2F%2BXs03gpo8vhKD4m3GFZga12U2IHVaj4DPrQJ5QiLTRxJPx1sQ9HAJqScK5huYhtYGhovobo1ngYjkVsovOr3cLDBB6LuQthmxjypOM7nc3MU5Up9rNvHIRxEQFOt4i%2Fgo7l3XS4Dp3wM3ZjgPyzeibr8cs4HuXDQTDceMlTkKSAPDL8A8gsZxM9EFJ0dW5r%2BO4i47Xkqz0kzOLG%2Bs90DCrlZOiBjq0AvA4%2BFj5LlC033Oam4dBAPAajPzM3SSOJL%2FFycgzZsqvFBs0WIeC8AP299xCq7wmV40Y0XW0QJxPb%2BHuZiTSP8rLmJ1%2BSjy6jpEs6jHeHHszK%2ByfVBBtKBJ4qetAnoKgwviaRTYXiMt1T7m5kkhJEx09xLAKKo8wQamJluDGwtzDm%2BUVSZ4kSGiAeQ%2F0jzAZdpEU2UOqO0UmU7uuo49CVOh9JQ7HqLZbzXJgrg7hbJ6r7DWqg04JhXlErHaIacj9bKQx%2BpB9d5d6S6lRmijxzf6bh46JlWRrmT6IJHhWj2Kka6QdliDALq%2BHf4yFaUN02AZqCrP%2B702YH4%2Fgqwms044f063Tj6kWCu56dEAxrvyMrTjbZIAPJBrsYw%2F5TCbR3hJp2dSe5mL7TVlnjv8TYbbIu3oB&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230423T062042Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIATDTNE5EASLDBSFRO%2F20230423%2Fap-northeast-1%2Fs3%2Faws4_request&X-Amz-Signature=b5608df740c39b8ec0334cd1d8bbcb3a622c75f5de58cf424ebf5dd2bdabcad9'
    # u_unix = r'https://bkt-dfk.s3.ap-northeast-1.amazonaws.com/workspace/forremote/tmp/test_unix.sql?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEOL%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaDmFwLW5vcnRoZWFzdC0yIkcwRQIgY6uPPitZvOtmc7ZFM1h4wR4E1mACKfFq3dNTosOjwm8CIQC%2FwUW2x2BPDiiyyJSPbUIuqPi9%2B%2BHBAbQ%2BQTyM8vOfFSqOAwjL%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAAaDDA2NDA1NTU0NTAxOCIMb84kPXnBvn6BnWtdKuICweO8WAB1mMpFOs9ruaantxkUjTmyYmJsb2c9ierXCyHv9c5nQR2AAlTGizQBCQ7eAyYgBTfrTUL7sP53jxIF7nlOsSdt%2B69zTNawdEyFvMn2BL3WsQjNVF%2FrjhlSbroOO%2FF5%2BeILa45SScDeYHdjk9Td3Ka3yCWkpdrq7TIGt9R7Mgfkit5t%2BwJK8xmOOb1hNyhSpmRCLAkqbjy40GO0S17Gytom9G9XKMvqI7LCDGFxgBbhqGrVLRsjTEoz7hxUU6n%2BODT2%2B0Evvb3LifDDeqOp46m7tJARfi%2B2x4JzPYIkR%2Brfefit5OFe%2FJyAGcaOmZrg%2F81uGaiTNRyA2UNobj8mV7EU9RIHIeHwKjnIqj1an6UICm1tmVrnSXUePJ2SnSEnnZ57%2BfhLD0yBmaFwpo8llU7L0YddNKY%2F2l60FRu10GUkR%2FPug6JWD95%2F450SsvEG3L67XmxHDj%2Fg%2BBK8E%2B6AMJyp3aEGOrMCyPxpH6t0eMmz5zjkS6jRlzM7RGSbuAe6jOsO1N7T2N7zoe7wj8Ij4jN3ebIeWPx8oo3CRB12TvAfDp7oTawDg1aMWAm42Qp8W47ioWiIQSlf1lwkzuLFRN%2FNk8afnJH2ZRBzY4U%2BM5b4efTiDMnPOZ6omN1YE0Dpz9YUK3fSX3bxlOsY7htx9U0YvBuw5I2aAV%2B%2F7LIJZoewwZXbw8BfC4FVtsqi%2FLIhMGU9FtULjBd4Q4xZMVGpaYZ%2Bi4aCDH9iEtrqIdU%2F%2FMUKdMP%2FIsVbkzoONXDBRBgQ9U6quAf0MNrFsbAZxbmgTBG0iTgZqkcsltW0xM0PF48IZQWzaJo0h0BEufWd7Tk5qIpJrUaHzD22w82oGMd0ufP681jGYc4EKCeWa93svnj3wcSl71m7mXqZIg%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230413T021243Z&X-Amz-SignedHeaders=host&X-Amz-Expires=28800&X-Amz-Credential=ASIAQ52QCBS5OKWLB2PK%2F20230413%2Fap-northeast-1%2Fs3%2Faws4_request&X-Amz-Signature=41a5a7a596ed033cd21a9e8bd8d8e0d6f12d12eab9dc340fd97a3c60b296ff10'
    # datetargetpath = r's3://lvdian-cedc-bucket/sales-target-data/'
    out_py_path = r'C:\LY\glue-script\demo.py'
    sql_file= r'C:\LY\sql\demo.sql'
    # gsg = GlueScriptGenerate('devops', u_unix, datetargetpath, out_py_path)
    # print(gsg)
    #print(gsg.get_script())
    # print(sys.argv[0])
    # print(sys.argv[1])
    # print(sys.argv[2])
    # print(sys.argv[3])
    default_params = {
        "database": 'database',
        "sql_path": u,
        "target_path": 'target_path',
        "out_py_path": out_py_path
    }
    gsg = GlueScriptGenerate(default_params)
    gsg.get_script()