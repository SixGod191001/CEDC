# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/3/11 17:57
"""
import abc
import random
import sql_metadata.compat


# 解析sql语句，提取出所有table name

class SqlParseInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_element(self):
        pass


class GetTables(SqlParseInterface):
    def __init__(self, sql_statement=None):
        """
        :param sql_statement: sql 语句
        :return: all table names in tuple
        """
        self.sql_statement = sql_statement

    def get_element(self):
        # 清洗数据，替换多余空白
        sqlstr = ' '.join(self.sql_statement.split())
        # 获取sql中的所有源表
        table_names = set(sql_metadata.compat.get_query_tables(sqlstr))
        return table_names


if __name__ == '__main__':
    sql = 'SELECT * FROM table1 JOIN table2 ON table1.id = table2.id WHERE table1.name = "foo";'
    g = GetTables(sql)
    tables = g.get_element()
    print(tables)

    sql2 = """SELECT *
            FROM orders
            JOIN (
              SELECT customer_id, MAX(order_date) AS latest_order_date
              FROM orders
              GROUP BY customer_id
            ) AS latest_orders
            ON orders.customer_id = latest_orders.customer_id
            AND orders.order_date = latest_orders.latest_order_date
            JOIN customers
            ON orders.customer_id = customers.customer_id
            """
    g = GetTables(sql2)
    tables = g.get_element()
    print(tables)
