# -*- coding: utf-8 -*-
import psycopg2
import logging
import json
from airflow_workspace.utils.secrets_manager_handler import SecretsManagerSecret
from airflow.exceptions import AirflowFailException  # make the task failed without retry
from airflow.exceptions import AirflowException  # failed with retry

logger = logging.getLogger(__name__)

class PostgreHandler:
    # 初始化
    def __init__(self):
        """
        变量释义如下：
        conn_info:       从secret manager获取连接信息
        """
        """从secret manager中获取到连接数据库的信息"""
        secret_manager_name = "cedc/dags/postgres"
        sm_info = json.loads(SecretsManagerSecret().get_cache_value(secret_name=secret_manager_name))
        self.dataBaseName = sm_info['dbname']
        self.userName = sm_info['username']
        self.password = sm_info['password']
        self.host = sm_info['host']
        self.port = sm_info['port']
        print("服务器:%s , 用户名:%s , 数据库:%s " % (self.host, self.userName, self.dataBaseName))
        # 连接数据库
        self._conn = self.get_connect()
        if self._conn:
            # 创建一个cursor  获取游标对象
            self._cur = self._conn.cursor()

    # 获取数据库连接对象
    def get_connect(self):
        conn = False
        try:
            conn = psycopg2.connect(
                database=self.dataBaseName,
                user=self.userName,
                password=self.password,
                host=self.host,
                port=self.port
            )
        except Exception as err:
            print("连接数据库失败，%s" % err)
        return conn

    # 执行查询sql
    def get_record(self, sql):
        """
        变量释义如下：
        sql:       查看数据的自定义的sql
        """
        res = ""
        try:
            self._cur.execute(sql)
            # 获取所有的数据
            res = self._cur.fetchall()
        except Exception as err:
            print("查询失败, %s" % err)
        else:
            return res
        self._cur.close()
        self._conn.close()
    # 执行insert
    def exce_insert(self, run_id=None, job_id=None, status=None):
        """
        变量释义如下：
        run_id:       job对应的执行的id
        job_id:       job的id
        status:       job的执行状态
        返回值：       0:成功 1:失败
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
                         WHERE JOB.JOB_ID={p_job_id} """
        sql = insert_sql.format(p_run_id=run_id, p_job_id=job_id, p_status=status)
        try:
            self._cur.execute(sql)
            self._conn.commit()
            flag = 0
        except Exception as err:
            flag = 1
            self._conn.rollback()
            print("执行失败, %s" % err)
        else:
            return flag
        self._cur.close()
        self._conn.close()
    # 执行update
    def exce_update(self, run_id=None, job_id=None, status=None):
        """
        变量释义如下：
        run_id:       job对应的执行的id
        job_id:       job的id
        status:       job的执行状态
        返回值：       0:成功 1:失败
        """
        update_sql = """UPDATE FACT_JOB_DETAILS SET JOB_END_DATE = CURRENT_TIMESTAMP, JOB_STATUS='{p_status}',
        LAST_UPDATE_DATE=CURRENT_TIMESTAMP WHERE RUN_ID ='{p_run_id}' AND JOB_ID = {p_job_id} """
        sql = update_sql.format(p_run_id=run_id, p_job_id=job_id, p_status=status)
        try:
            self._cur.execute(sql)
            self._conn.commit()
            flag = 0
        except Exception as err:
            flag = 1
            self._conn.rollback()
            print("执行失败, %s" % err)
        else:
            return flag
        self._cur.close()
        self._conn.close()
    # 执行delete
    def exce_delete(self, run_id=None):
        """
        变量释义如下：
        run_id:       job对应的执行的id
        返回值：       0:成功 1:失败
        """

        delete_sql = """ DELETE FROM FACT_JOB_DETAILS WHERE RUN_ID ='{p_run_id}' """
        sql = delete_sql.format(p_run_id=run_id)
        try:
            self._cur.execute(sql)
            self._conn.commit()
            flag = 0
        except Exception as err:
            flag = 1
            self._conn.rollback()
            print("执行失败, %s" % err)
        else:
            return flag
        self._cur.close()
        self._conn.close()
if __name__ == "__main__":

    run_id = "34567"
    job_id = "1005"
    status = "running"
    conn = PostgreHandler()
    response = conn.exce_insert(run_id=run_id, job_id=job_id, status=status)
    #response = conn.exce_update(run_id=run_id, job_id=job_id, status=status)
    #response = conn.exce_delete(run_id=run_id)
    print(response)
    # 查看查询结果
    Query_SQL = """ SELECT * FROM FACT_JOB_DETAILS WHERE RUN_ID = '{p_run_id}' """
    rows = conn.get_record(Query_SQL.format(p_run_id=run_id))
    for row in rows:
        print(row)
