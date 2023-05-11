"""
@Author : Yuwei
@Date : 2023/5/11 11:31
"""

import json
from airflow_workspace.utils.logger_handler import logger
from airflow.exceptions import AirflowFailException, AirflowException
from airflow_workspace.utils.postgre_handler import PostgresHandler

logger = logger()

# 初始化数据库
pg_handler = PostgresHandler()

class PublishData:
    """
    发布表
    :param json_path: json文件路径
    :param table_name: 表名
    """

    def __init__(self,json_path,table_name):
        # 判断json_path去掉.json之后和table_name是否相等
        self.json_name = json_path.split('/')[-1].split('.')[0]
        if self.json_name!= table_name:
            logger.error(f'json文件{self.json_name}和表{table_name}不一致，请检查！')
            raise AirflowFailException(f'json文件{self.json_name}和表{table_name}不一致，请检查！')
        
        self.json_path = json_path
        self.table_name = table_name
    
    # 读取当前路径下的json文件，返回json数据
    def open_data(self):
        try:
            # 读取json文件
            with open(self.json_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
                if isinstance(json_data, dict):
                    logger.info(f'当前json文件数据：{json_data}')
                    return json_data
                else:
                    logger.error(f'无法解析JSON文件: {self.json_path}，文件内容不是一个有效的字典')
                    return None
        except (FileNotFoundError, IOError) as e:
            logger.error(f'无法读取JSON文件: {self.json_path}，错误信息: {str(e)}')
            return None

    
    # 根据json数据，向dim_dag表中插入数据,根据dag_name判断是否存在，存在删除插入，不存在直接插入
    def insert_data_dim_dag(self):
        # 读取json数据
        json_data = self.open_data()
        if json_data is None:
            logger.error(f'json文件{self.json_name}中没有数据，请检查！')
            raise AirflowFailException(f'json文件{self.json_name}中没有数据，请检查！')

        conn = PostgresHandler()

        dag_name = json_data.get('dag_name')

        # 判断是否存在记录
        exist_sql = f"SELECT count(*) FROM dim_dag WHERE dag_name = '{dag_name}'"
        result = conn.get_record(exist_sql.format(dag_name=dag_name))
        # Query_SQL = """ SELECT * FROM FACT_JOB_DETAILS WHERE RUN_ID = '{p_run_id}' """
        # rows = conn.get_record(Query_SQL.format(p_run_id=run_id))
        for row in result:
            logger.info(row)
        # if result[0][0] > 0:
        #     # 存在记录，则先删除
        #     delete_sql = f"DELETE FROM dim_dag WHERE dag_name = '{dag_name}'"
        #     conn.execute_delete(delete_sql)

        # # 插入新数据
        # insert_sql = """
        #     INSERT INTO dim_dag (dag_id, dag_name, trigger_type, check_dependence, tag, insert_date, last_update_date, dag_version)
        #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        # """
        # values = (
        #     json_data.get('dag_id'),
        #     dag_name,
        #     json_data.get('trigger_type'),
        #     json_data.get('is_check_dependence'),
        #     json_data.get('tag'),
        #     json_data.get('insert_date'),
        #     json_data.get('last_update_date'),
        #     json_data.get('dag_version')
        # )
        # conn.execute_insert(insert_sql, values)



            
if __name__ == '__main__':
    publish_data = PublishData('dim_dag.json','dim_dag')
    publish_data.insert_data_dim_dag()









    
    







