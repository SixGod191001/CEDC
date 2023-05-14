# -*- coding: utf-8 -*-
import psycopg2
import json
from airflow_workspace.utils.secrets_manager_handler import SecretsManagerSecret
# make the task failed without retry
from airflow.exceptions import AirflowFailException
from airflow.exceptions import AirflowException  # failed with retry
from airflow_workspace.utils import logger_handler

logger = logger_handler.logger()


class PostgresHandler:
    def __init__(self, secret_manager_name="cedc/dags/postgres"):
        """
        @desc: 从secret manager中获取到连接数据库的信息
        :param secret_manager_name: aws secret manager name
        """
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

    # 获取数据库连接对象
    def get_connect(self):
        try:
            conn = psycopg2.connect(
                database=self.dataBaseName,
                user=self.userName,
                password=self.password,
                host=self.host,
                port=self.port
            )
        except Exception as err:
            logger.error("连接数据库失败，%s" % err)
            raise AirflowException("get connect is bad!")
        return conn

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
            self._conn.close()

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
            self._conn.close()
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
                         (DAG_ID,TASK_ID,JOB_ID,RUN_ID,JOB_START_DATE,JOB_END_DATE,JOB_STATUS,INSERT_DATE,LAST_UPDATE_DATE)
                         SELECT DAG.DAG_ID 
                               ,TASK.TASK_ID 
                               ,JOB.JOB_ID 
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
            self._conn.close()
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
            self._conn.close()
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
            self._conn.close()
            return flag


if __name__ == "__main__":
    run_id = "1"
    job_name = "cedc_sales_prelanding_job1"
    status = "running"
    conn = PostgresHandler()
    # response = conn.execute_insert(run_id=run_id, job_name=job_name, status=status)
    # response = conn.execute_update(ded=run_id, job_name=job_name, status=status)
    # response = conn.execute_delete(run_id=run_id)
    # logger.info(response)
    # 查看查询结果
    Query_SQL = """ SELECT * FROM FACT_JOB_DETAILS WHERE RUN_ID = '{p_run_id}' """
    rows = conn.get_record(Query_SQL.format(p_run_id=run_id))
    print(rows)
    # for row in rows:
    #     logger.info(row)
