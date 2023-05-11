"""
@Author : Yuwei
@Date : 2023/5/11 11:31
"""

import json
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
        # 判断json_path去掉.json之后和table_name是否相等
        self.json_name = json_path.split('/')[-1].split('.')[0]
        if self.json_name != table_name:
            logger.error(f'json文件{self.json_name}和表{table_name}不一致，请检查！')
            raise AirflowFailException(
                f'json文件{self.json_name}和表{table_name}不一致，请检查！')

        self.json_path = json_path
        self.table_name = table_name

    # 读取当前路径下的json文件，返回json数据
    def open_data(self):
        try:
            # 读取json文件
            with open(self.json_path, 'r', encoding='utf-8') as json_file:
                json_data = json.load(json_file)
                return json_data
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

        for item in json_data:
            dag_name = item['dag_name']
            logger.info(f'查询的{dag_name}')
            # # 判断是否存在记录
            exist_sql = f"SELECT count(*) FROM dim_dag WHERE dag_name = '{dag_name}';"
            logger.info(f'查询SQL：{exist_sql}')
            result = conn.get_record(exist_sql.format(dag_name=dag_name))
            logger.info(f'查询结果：{result}')
            if result[0][0] > 0:
                # 存在记录，则先删除
                delete_sql = f"DELETE FROM dim_dag WHERE dag_name = '{dag_name}'"
                logger.info(f'删除数据SQL：{delete_sql}')
                conn.execute_delete(delete_sql)

            # 插入新数据
            insert_sql = """
                INSERT INTO dim_dag (dag_name, description, owner, default_view, trigger_type, schedule_interval, next_dagrun, is_check_dependence, concurrency, tag, fileloc, is_active, insert_date, last_update_date, dag_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                item['dag_name'],
                item['description'],
                item['owner'],
                item['default_view'],
                item['trigger_type'],
                item['schedule_interval'],
                item['next_dagrun'],
                item['is_check_dependence'],
                item['concurrency'],
                item['tag'],
                item['fileloc'],
                item['is_active'],
                item['insert_date'],
                item['last_update_date'],
                item['dag_version']
            )
            conn.execute_insert(insert_sql, values)
            logger.info(f'插入数据成功！')


if __name__ == '__main__':
    publish_data = PublishData(
        '/workspaces/CEDC/airflow_workspace/metadata_db/DML/dim_dag.json', 'dim_dag')
    publish_data.insert_data_dim_dag()
