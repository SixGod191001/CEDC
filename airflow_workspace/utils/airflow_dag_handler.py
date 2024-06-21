# -*- coding: utf-8 -*-
"""
@Author : Liu Zhu
@Date : 2023/5/9 16:31
"""
import requests
from datetime import timedelta
from airflow.exceptions import AirflowFailException  # make the task failed without retry
from airflow.exceptions import AirflowException  # failed with retry
from airflow_workspace.utils.logger_handler import logger
from datetime import datetime
from airflow_workspace.utils.postgre_handler import PostgresHandler

logger = logger()


class AirflowDagHandler:
    def __init__(self, base_url):
        self.base_url = base_url

    def get_dag_info(self, dag_id):
        """
        通过DB获取指定 DAG 的维度信息
        参数:
        - dag_id：要查询的 DAG 的 name
        返回值:
        - result 
        """
        postgres_handler = PostgresHandler()
        sql = f"SELECT * FROM dim_dag WHERE dag_name = '{dag_id}' AND is_active = 'Y'"
        logger.info(f'查询SQL：{sql}')
        result = postgres_handler.execute_select(sql.format(dag_id=dag_id))
        logger.info(f'查询结果：{result}')
        
        if result is not None:
            return result  # 返回
        else:
            raise AirflowFailException(f'通过DB没有查询到的{dag_id}的维度信息,请检查')

    def get_dependencies_dag_ids_by_db(self, dag_id):
        """
        通过数据库查询获取指定 DAG 的依赖 DAG ID 列表
        参数:
        - dag_id: 要查询依赖关系的 DAG 的 name
        返回值:
        - 依赖 DAG ID 的列表
        """
        # 使用 PostgresHandler 执行查询
        postgres_handler = PostgresHandler()
        sql = f"SELECT DISTINCT dependency_dag_name FROM dim_dag_dependence WHERE dag_name = '{dag_id}' AND is_active = 'Y'"
        result = postgres_handler.execute_select(sql.format(dag_id=dag_id))

        # 解析查询结果并返回依赖 DAG ID 的列表
        dag_ids = [row['dependency_dag_name'] for row in result] if result else []
        return dag_ids

    def get_dag_state_by_db(self, dag_id):
        """
        通过数据库查询指定 DAG 的最新运行状态
        参数:
        - dag_id: 要查询依赖关系的 DAG 的 ID
        返回值:
        - result [('dag_id', 'state')]
        """

        conn = PostgresHandler()

        sql = f"""  select * from public.fact_dag_details where dag_name = '{dag_id}' 
                    order by execution_date desc limit 1;
               """

        logger.info(f'查询SQL：{sql}')
        result = conn.execute_select(sql.format(dag_id=dag_id))

        logger.info(f'查询结果：{result}')

        if result is not None:
            return result  # 返回
        else:
            logger.error(f'通过DB没有查询到的{dag_id}的最新运行记录')
            raise AirflowFailException(f'通过DB没有查询到的{dag_id}的最新运行记录')

    def get_dag_state_by_api(self, dag_id):
        """
        通过API获取指定 DAG 的state
        参数:
        - dag_id: 要查询DAG 的 ID
        返回值:
        - dag_state: 所查询Dag的state
        """

        # 构造查询参数
        current_datetime = datetime.now()  # 默认查询当天的dag运行状态
        formatted_datetime = current_datetime.strftime("%Y-%m-%d")
        # 将格式化后的日期字符串转换为 datetime 对象
        converted_datetime = datetime.strptime(formatted_datetime, "%Y-%m-%d")
        execution_date = converted_datetime
        start_date_str = execution_date.isoformat() + 'Z'
        # end_date_str = (execution_date + timedelta(days=1) - timedelta(seconds=1)).isoformat() + 'Z'

        # 构造请求URL和参数
        dag_run_api_url = "{}/api/v1/dags/{}/dagRuns?execution_date_gte={}&order_by=-execution_date&limit=1".format(
            self.base_url, dag_id, start_date_str)
        logger.info(f'请求的url: {dag_run_api_url}')
        print("1")
        # 发起请求
        headers = {'Authorization': 'Basic YWlyZmxvdzphaXJmbG93'}
        response = requests.get(dag_run_api_url, headers=headers)
        print("2")
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise AirflowFailException(f"Error occurred while fetching DAG state. Response code: {response.status_code}. Error message: {e}")

        dag_runs = response.json()['dag_runs']

        if not dag_runs:
            print("3.1")
            logger.error(f'通过API没有查询到{formatted_datetime}这天的{dag_id}运行记录')
            raise AirflowFailException(f'通过API没有查询到{formatted_datetime}这天的{dag_id}运行记录')
            print("3.2")
        print("4")
        # 获取state
        last_dag_run = dag_runs[0]
        dag_state = last_dag_run['state']
        print("5")
        return dag_state


if __name__ == '__main__':
    # 创建AirflowDagUtils实例
    dag_handler = AirflowDagHandler("http://43.143.250.12:8080")

    # # 通过DB查询具有dependency的Dag_ids
    # Dag_ids = dag_handler.get_dependencies_dag_ids_by_db('dag_cedc_sales_pub')
    # print(Dag_ids)

    ## 通过API获取DAG状态
    dag_state_by_api = dag_handler.get_dag_state_by_api("dag_cedc_sales_landing")
    print(dag_state_by_api)

    # #通过DB获取DAG状态
    # dag_state_by_db = dag_handler.get_dag_state_by_db("dag_cedc_sales_landing")
    # print(dag_state_by_db)
    # search_dependency_dagname = dag_state_by_db[0]['dag_name']
    # search_dependency_dag_state = dag_state_by_db[0]['status']
    # print(search_dependency_dagname)
    # print(search_dependency_dag_state)










