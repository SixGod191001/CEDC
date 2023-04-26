# -*- coding: utf-8 -*-
import psycopg2
import argparse
import logging
import sys
#import boto3
#from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class PostgreHandler:
    # 初始化
    def __init__(self, dataBaseName, userName, password, host, port):
        self.dataBaseName = dataBaseName
        self.userName = userName
        self.password = password
        self.host = host
        self.port = port

        self._conn = self.GetConnect()
        if self._conn:
            self._cur = self._conn.cursor()

    # 获取数据库连接对象
    def GetConnect(self):
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
    #关闭数据库连接
    def CloseCnnect(self):
        #提交事务
        self._conn.commit()
        #关闭连接
        self._conn.close()
    # 执行查询sql
    def GetRecord(self, sql):
        res = ""
        try:
            self._cur.execute(sql)
            res = self._cur.fetchall()
        except Exception as err:
            print("查询失败, %s" % err)
        else:
            return res

    # 执行增删改sql
    def ExceDMLQuery(self, sql):
        flag = False
        try:
            self._cur.execute(sql)
            self._conn.commit()
            flag = True
        except Exception as err:
            flag = False
            self._conn.rollback()
            print("执行失败, %s" % err)
        else:
            return flag

    def GetConnectInfo(self):
        print("连接信息：")
        print("服务器:%s , 用户名:%s , 数据库:%s " % (self.host, self.userName, self.dataBaseName))

Insert_SQL = """ INSERT INTO DIM_DAG (DAG_ID,DAG_NAME,TRIGGER_TYPE,CHECK_DEPENDENCE,TAG,INSERT_DATE,LAST_UPDATE_DATE,DAG_VERSION) VALUES ({p_dag_id},'{p_dag_name}','GLUE','N','TEST',CURRENT_DATE,CURRENT_DATE,1) """
Update_SQL = """ UPDATE DIM_DAG SET DAG_NAME = '{p_dag_name}' WHERE DAG_ID ='{p_dag_id}' """
Delete_SQL = """ DELETE FROM DIM_DAG WHERE DAG_ID ='{p_dag_id}' """
Query_SQL = """ SELECT * FROM DIM_DAG WHERE DAG_ID = {p_dag_id} """

if __name__ == "__main__":
    """
    调用示例如下： 
    python main.py --trigger=start_batch --params='{"datasource_name": "sample", "load_type": "ALL", "run_type": "glue", "glue_template_name":"cedc_sales_prelanding_template"}'
    python p.py start_batch name id
    变量释义如下：
    dataBaseName: 数据库名称
    userName:     访问数据库用户名
    password:     访问数据库密码
    host:         数据库所在服务器
    port:         访问数据库端口
    DMLObject:    数据库操作对象()
    DMLType：     数据库操作类型(I:新增; U:更新; D:删除; S:查询)
    """
    # get parameters for database connection
    parser = argparse.ArgumentParser(description='Get Database Connection info')
    parser.add_argument("--dataBaseName", type=str, default='postgreDB')
    parser.add_argument("--userName", type=str,default='postgres')
    parser.add_argument("--password", type=str, default='password123')
    parser.add_argument("--host", type=str, default='database-1.cw7feqnaopjp.ap-northeast-1.rds.amazonaws.com')
    parser.add_argument("--port", type=str, default='5432')
    parser.add_argument("--DMLObject", type=str, default='123')
    parser.add_argument("--DMLType", type=str, default='U')

    args = parser.parse_args()
    dataBaseName=args.dataBaseName
    userName = args.userName
    password = args.password
    host = args.host
    port = args.port
    DMLType = args.DMLType

    dag_id = 345
    dag_name = "Dag003"
    DMLType = "I"
    #连接Database
    conn = PostgreHandler(dataBaseName, userName, password,host, port)
    #获取到Database 连接信息
    conn.GetConnectInfo()
    #执行数据库操作SQL
    if DMLType == "I":
        conn.ExceDMLQuery(Insert_SQL.format(p_dag_id=dag_id,p_dag_name=dag_name))
    if DMLType == "U":
        conn.ExceDMLQuery(Update_SQL.format(p_dag_id=dag_id,p_dag_name=dag_name))
    if DMLType == "D":
        conn.ExceDMLQuery(Delete_SQL.format(p_dag_id=dag_id))
    #查看查询结果
    rows = conn.GetRecord(Query_SQL.format(p_dag_id=dag_id))
    for row in rows:
        print(row)
    conn.CloseCnnect()
