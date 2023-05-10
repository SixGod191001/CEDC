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
from glue_workspace.script.utils import strtool, constants, filetool

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
    def __init__(self, default_params=None):
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
        sql = ft.read_file()
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

    database = sys.argv[1]
    u = sys.argv[2]
    target_path = sys.argv[3]
    out_py_path = sys.argv[4]
    default_params = {
        "database": 'database',
        "sql_path": u,
        "target_path": 'target_path',
        "out_py_path": out_py_path
    }
    gsg = GlueScriptGenerate(default_params)
    gsg.get_script()