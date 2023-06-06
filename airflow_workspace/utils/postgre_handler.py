# -*- coding: utf-8 -*-

import datetime
import json

import psycopg2
# make the task failed without retry
from airflow.exceptions import AirflowException  # failed with retry
from dbutils.pooled_db import PooledDB

from airflow_workspace.utils import logger_handler
from airflow_workspace.utils.secrets_manager_handler import SecretsManagerSecret

logger = logger_handler.logger()


class PostgresHandler:
    def __init__(self, secret_manager_name="cedc/dags/postgres"):
        """
        @desc: 从secret manager中获取到连接数据库的信息
        :param secret_manager_name: aws secret manager name
        """
        self._pool = None
        sm_info = json.loads(SecretsManagerSecret().get_cache_value(secret_name=secret_manager_name))
        self.dataBaseName = sm_info['dbname']
        self.userName = sm_info['username']
        self.password = sm_info['password']
        self.host = sm_info['host']
        self.port = sm_info['port']
        logger.info("服务器:%s , 用户名:%s , 数据库:%s " % (self.host, self.userName, self.dataBaseName))
        # 连接数据库
        self._conn = self.get_connect()
        if self._conn:
            # 创建一个cursor  获取游标对象
            self._cur = self._conn.cursor()

    def init_pgsql_pool(self):
        '''初始化连接池
        '''
        try:
            logger.info('Begin to create {0} postgresql pool on：{1}.\n'.format(self.host, datetime.datetime.now()))
            pool = PooledDB(
                creator=psycopg2,  # 使用连接数据库的模块 psycopg2
                maxconnections=6,  # 连接池允许的最大连接数，0 和 None 表示不限制连接数
                mincached=1,  # 初始化时，链接池中至少创建的空闲的链接，0 表示不创建
                maxcached=2,  # 链接池中最多闲置的链接，0 和 None 不限制
                blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
                maxusage=None,  # 一个链接最多被重复使用的次数，None 表示无限制
                setsession=[],  # 开始会话前执行的命令列表
                host=self.host,
                port=self.port,
                user=self.userName,
                password=self.password,
                database=self.dataBaseName)
            self._pool = pool
            logger.info(
                'SUCCESS: create {0} postgresql pool success on {1}.\n'.format(self.host, datetime.datetime.now()))

        except Exception as e:
            logger.error(
                'ERROR: create {0} postgresql pool failed on {1}.\n'.format(self.host, datetime.datetime.now()))
            raise AirflowException('ERROR: create postgresql pool error caused by {0}'.format(str(e)))

    # 获取数据库连接对象
    def get_connect(self):

        if not self._pool:
            self.init_pgsql_pool()
        return self._pool.connection()

    def get_table_columns(self, table_name):
        try:
            query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
            """
            self._cur.execute(query, (table_name,))
            columns = [column[0] for column in self._cur.fetchall()]
            return columns
        except Exception as err:
            logger.error(f"无法获取表 {table_name} 的列信息, {err}")
            raise AirflowException("获取表列信息失败！")
        finally:
            self._cur.close()
            # self._conn.close()

    # 执行查询sql

    def get_record(self, sql):
        """
        :param sql: 查看数据的自定义的sql
        :return: dict_results = [{"column name":"value","column name":"value"},{"column name":"value","column name":"value"}]
        """

        res = ""
        try:
            self._cur.execute(sql)
            # 获取所有的数据
            res = self._cur.fetchall()
            dict_results = []

            for row in res:
                dict_row = {}
                for i in range(len(self._cur.description)):
                    dict_row[self._cur.description[i].name] = row[i]
                dict_results.append(dict_row)
        except Exception as err:
            logger.error("查询失败, %s" % err)
            raise AirflowException("get record is bad!")
        else:
            self._cur.close()
            # self._conn.close()
            return dict_results

    # 执行insert
    def execute_insert(self, run_id=None, job_name=None, status=None):
        """
        :param run_id: job对应的执行的id
        :param job_name:
        :param status: job的执行状态
        :return: 当没有数据insert的时候会返回 9 ，insert成功时返回 0， 失败时返回 1
        """
        insert_sql = """ INSERT INTO FACT_JOB_DETAILS 
                         (DAG_NAME,TASK_NAME,JOB_NAME,RUN_ID,JOB_START_DATE,JOB_END_DATE,JOB_STATUS,INSERT_DATE,LAST_UPDATE_DATE)
                         SELECT DAG.DAG_NAME 
                               ,TASK.TASK_NAME 
                               ,JOB.JOB_NAME 
                               ,'{p_run_id}' AS RUN_ID 
                               ,CURRENT_TIMESTAMP AS JOB_START_DATE 
                               ,NULL AS JOB_END_DATE 
                               ,'{p_status}' AS JOB_STATUS 
                               ,CURRENT_TIMESTAMP AS INSERT_DATE 
                              ,CURRENT_TIMESTAMP AS LAST_UPDATE_DATE 
                         FROM DIM_JOB JOB 
                         INNER JOIN DIM_TASK TASK ON JOB.TASK_NAME=TASK.TASK_NAME 
                         INNER JOIN DIM_DAG DAG ON TASK.DAG_NAME=DAG.DAG_NAME 
                         WHERE JOB.JOB_NAME='{p_job_name}' """
        sql = insert_sql.format(p_run_id=run_id, p_job_name=job_name, p_status=status)

        try:
            self._cur.execute(sql)
            self._conn.commit()
            rowcount = self._cur.rowcount
            if rowcount >= 1:
                flag = 0
            else:
                flag = 9
        except Exception as err:
            flag = 1
            self._conn.rollback()
            logger.error("执行失败, %s" % err)
            raise AirflowException("execute_insert is bad!")
        else:
            self._cur.close()
            # self._conn.close()
            return flag

    # 执行update
    def execute_update(self, run_id=None, job_name=None, status=None):
        """
        :param run_id: job对应的执行的id
        :param job_name: job名字
        :param status: job的执行状态
        :return: 当没有数据update的时候会返回 9 ，update成功时返回 0， 失败时返回 1
        """
        update_sql = """UPDATE FACT_JOB_DETAILS SET JOB_END_DATE = CURRENT_TIMESTAMP, JOB_STATUS='{p_status}',
        LAST_UPDATE_DATE=CURRENT_TIMESTAMP WHERE RUN_ID ='{p_run_id}' AND JOB_NAME = '{p_job_name}' """
        sql = update_sql.format(p_run_id=run_id, p_job_name=job_name, p_status=status)

        try:
            self._cur.execute(sql)
            self._conn.commit()
            rowcount = self._cur.rowcount
            if rowcount >= 1:
                flag = 0
            else:
                flag = 9
        except Exception as err:
            flag = 1
            self._conn.rollback()
            logger.error("执行失败, %s" % err)
            raise AirflowException("execute_update is bad!")
        else:
            self._cur.close()
            # self._conn.close()
            return flag

    # 执行delete
    def execute_delete(self, run_id=None):
        """
        :param run_id: job对应的执行的id
        :return: 当没有数据delete的时候会返回 9 ，delete成功时返回 0， 失败时返回 1
        """
        delete_sql = """ DELETE FROM FACT_JOB_DETAILS WHERE RUN_ID ='{p_run_id}' """
        sql = delete_sql.format(p_run_id=run_id)
        try:
            self._cur.execute(sql)
            self._conn.commit()
            rowcount = self._cur.rowcount
            if rowcount >= 1:
                flag = 0
            else:
                flag = 9
        except Exception as err:
            flag = 1
            self._conn.rollback()
            logger.error("执行失败, %s" % err)
            raise AirflowException("execute_delete is bad!")
        else:
            self._cur.close()
            # self._conn.close()
            return flag

    def execute_sql(self, sql):
        """
        :param run_id: job对应的执行的id
        :param job_name: job名字
        :param status: job的执行状态
        :return: 当没有数据update的时候会返回 9 ，update成功时返回 0， 失败时返回 1
        """
        try:
            self._cur.execute(sql)
            self._conn.commit()
            rowcount = self._cur.rowcount
            if rowcount >= 1:
                flag = 0
            else:
                flag = 9
        except Exception as err:
            flag = 1
            self._conn.rollback()
            logger.error("执行失败, %s" % err)
            raise AirflowException("execute_update is bad!")
        else:
            self._cur.close()
            # self._conn.close()
            return flag

    def close_pool(self):
        '''关闭 pool
        '''
        if self._pool != None:
            self._pool.close()

    def task_execute_update(self, task_name=None, status=None):
        """
        :param run_id: job对应的执行的id
        :param task_name: job名字
        :param status: job的执行状态
        :return: 当没有数据update的时候会返回 9 ，update成功时返回 0， 失败时返回 1
        """
        # update_sql = """UPDATE FACT_TASK_DETAILS SET END_DATE = CURRENT_TIMESTAMP, STATUS='{p_status}',
        # LAST_UPDATE_DATE=CURRENT_TIMESTAMP WHERE RUN_ID ='{p_run_id}' AND task_name = '{p_task_name}' """

        update_sql = """
        UPDATE fact_task_details
        SET END_DATE = CURRENT_TIMESTAMP, status = '{p_status}',LAST_UPDATE_DATE=CURRENT_TIMESTAMP
        WHERE start_date = (SELECT MAX(start_date) FROM fact_task_details WHERE  task_name='{p_task_name}') and task_name='{p_task_name}'
        """
        sql = update_sql.format(p_task_name=task_name, p_status=status)

        try:
            self._cur.execute(sql)
            self._conn.commit()
            rowcount = self._cur.rowcount
            if rowcount >= 1:
                flag = 0
            else:
                flag = 9
        except Exception as err:
            flag = 1
            self._conn.rollback()
            logger.error("执行失败, %s" % err)
            raise AirflowException("execute_update is bad!")
        else:
            self._cur.close()
            # self._conn.close()
            return flag

    def dag_execute_update(self, dag_name=None, status=None):

        update_sql = """
        UPDATE fact_dag_details
        SET status = '{p_status}',LAST_UPDATE_DATE=CURRENT_TIMESTAMP
        WHERE  dag_name='{p_dag_name}' and start_date = (SELECT MAX(start_date) FROM fact_dag_details WHERE  dag_name = '{p_dag_name}');
        """
        sql = update_sql.format(p_dag_name=dag_name, p_status=status)

        try:
            self._cur.execute(sql)
            self._conn.commit()
            rowcount = self._cur.rowcount
            if rowcount >= 1:
                flag = 0
            else:
                flag = 9
        except Exception as err:
            flag = 1
            self._conn.rollback()
            logger.error("执行失败, %s" % err)
            raise AirflowException("execute_update is bad!")
        else:
            self._cur.close()
            # self._conn.close()
            return flag



if __name__ == "__main__":
    run_id = "1"
    job_name = "cedc_sales_prelanding_job1"
    status = "running"
    conn = PostgresHandler()
    # response = conn.execute_insert(run_id=run_id, job_name=job_name, status=status)
    # response = conn.execute_update(run_id=run_id, job_name=job_name, status=status)
    # response = conn.execute_delete(run_id=run_id)
    # logger.info(response)
    # update_sql = """UPDATE FACT_JOB_DETAILS SET JOB_END_DATE = CURRENT_TIMESTAMP WHERE id = 1 """
    # response11 = conn.execute_sql(update_sql)
    # print(response)
    # 查看查询结果
    Query_SQL = """ SELECT * FROM FACT_JOB_DETAILS WHERE RUN_ID = '{p_run_id}' """
    rows = conn.get_record(Query_SQL.format(p_run_id=run_id))
    print(rows)
    response = conn.execute_update(run_id=run_id, job_name=job_name, status=status)
    print(response)
