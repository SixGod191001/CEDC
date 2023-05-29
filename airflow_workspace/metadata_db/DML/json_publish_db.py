"""
@Author : Yuwei
@Date : 2023/5/11 11:31
"""
import json
import sys
from airflow_workspace.utils.logger_handler import logger
from airflow.exceptions import AirflowFailException, AirflowException
from airflow_workspace.utils.postgre_handler import PostgresHandler

logger = logger()

class PublishData:
    """
    发布数据
    :param json_path: json数据文件路径
    :param table_name: 表名
    """

    def __init__(self, json_path, table_name):
        self.json_path = json_path
        self.table_name = table_name
        

    # 解析当前路径下的json文件，返回json数据
    def parse_json_file(self):
        try:
            with open(self.json_path, 'r', encoding='utf-8') as json_file:
                json_data = json.load(json_file)
                return json_data
        except (FileNotFoundError, IOError) as e:
            logger.info(f'无法读取JSON文件: {self.json_path}，错误信息: {str(e)}')
            return None

    def generate_insert_query(self, table_name):
        json_data = self.parse_json_file()
        if json_data is None:
            return None
        
        conn = PostgresHandler()
        # 获取数据库表的字段列表
        table_columns = conn.get_table_columns(table_name)
        
        if table_name in json_data:
            json_table_data = json_data[table_name]
            json_columns = list(json_table_data[0].keys())
            
            # 比较字段列表是否一致
            if set(json_columns) != set(table_columns):
                # 找到不一致的字段
                diff_columns = set(json_columns).symmetric_difference(set(table_columns))
                error_message = f"JSON数据字段与数据库表{table_name}字段不一致: {diff_columns}"
                raise ValueError(error_message)
            
            # 构建插入查询
            insert_query = f"INSERT INTO {table_name} ({', '.join(json_columns)}) VALUES "
            
            # 获取插入的值
            values = []
            for row in json_table_data:
                row_values = ', '.join(f"'{value}'" for value in row.values())
                values.append(f"({row_values})")
            
            insert_query += ', '.join(values)
        else:
            insert_query = None

        return insert_query



    # # 根据json数据，向dim_dag表中插入数据,根据dag_name判断是否存在，存在删除插入，不存在直接插入

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Please provide json_path and table_name as command-line arguments.")
        sys.exit(1)
    
    conn = PostgresHandler()
    json_path = sys.argv[1]
    table_name = sys.argv[2]
    
    # 创建PublishData实例
    publish_data = PublishData(json_path=json_path, table_name=table_name)

    # 调用generate_insert_query方法生成插入查询
    insert_query = publish_data.generate_insert_query(table_name=table_name)
    logger.info(f'插入语句：{insert_query}')
    flag = conn.execute_sql(insert_query)

    if flag == 0:
        logger.info("插入成功")
    elif flag == 9:
        logger.info("没有数据插入")
    else:
        logger.info("插入失败")


    




