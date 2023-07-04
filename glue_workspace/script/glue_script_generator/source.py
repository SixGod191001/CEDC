# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/3/9 22:23
"""
import abc
import random


# 生成 Glue 数据源的代码

class DatasourceInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def create_dynamic_frame(self):
        pass


class CsvDatasource(DatasourceInterface):
    def __init__(self, quotechar=None, separator=None, source_path=None, withHeader=None):
        """
        :param quotechar: quotechar
        :param separator: separator
        :param source_path: source_path
        """
        self.quotechar = quotechar
        self.separator = separator
        self.source_path = source_path
        self.withHeader = withHeader
        self.transformation_ctx = "S3bucket_node{random_id}".format(
            random_id=random.randint(1000000000001,
                                     1999999999999))

    def create_dynamic_frame(self):
        comment = "# Script generated for node Csv\n"
        sql = '''{transformation_ctx} = glueContext.create_dynamic_frame.from_options(
                   format_options={{"quoteChar": '{quotechar}', "withHeader": {withHeader}, "separator": "{separator}"}},
                   connection_type="s3",
                   format="csv",
                   connection_options={{
                       "paths": ["{source_path}"],
                       "recurse": True,
                   }},
                   transformation_ctx="{transformation_ctx}",
        )'''.format(quotechar=self.quotechar, separator=self.separator, source_path=self.source_path,
                   withHeader=self.withHeader, transformation_ctx=self.transformation_ctx)
        print(comment + sql)
        return self.transformation_ctx, comment + sql


class RelationDBDatasource(DatasourceInterface):
    def create_dynamic_frame(self):
        pass


class RedshiftDatasource(DatasourceInterface):
    def create_dynamic_frame(self):
        pass


class MySQLDatasource(DatasourceInterface):
    def __init__(self, database=None, table_name=None):
        """
        :param database: database
        :param table_name: table_name
        """
        self.database = database
        self.table_name = table_name
        self.transformation_ctx = "DatabaseNode_node{random_id}".format(
            random_id=random.randint(1000000000001,
                                     1999999999999))

    def create_dynamic_frame(self):
        comment = "# Script generated for node MySQL\n"
        sql = '''{transformation_ctx} = glueContext.create_dynamic_frame.from_options(
                connection_type="mysql",
                connection_options={{
                    "useConnectionProperties": "true",
                    "dbtable":"{table_name}",
                    "connectionName":"{database}"
                }},
                transformation_ctx="{transformation_ctx}",
    )'''.format(database=self.database, table_name=self.table_name, transformation_ctx=self.transformation_ctx)
        print(comment + sql)
        return self.transformation_ctx, comment + sql


class PostgreSQLDatasource(DatasourceInterface):
    def __init__(self, database=None, table_name=None):
        """
        :param database: database
        :param table_name: table_name
        """
        self.database = database
        self.table_name = table_name
        self.transformation_ctx = "DatabaseNode_node{random_id}".format(
            random_id=random.randint(1000000000001,
                                     1999999999999))

    def create_dynamic_frame(self):
        comment = "# Script generated for node PostgreSQL\n"
        sql = '''{transformation_ctx} = glueContext.create_dynamic_frame.from_options(
                connection_type="postgresql",
                connection_options={{
                    "useConnectionProperties": "true",
                    "dbtable":"{table_name}",
                    "connectionName":"{database}"
                }},
                transformation_ctx="{transformation_ctx}",
    )'''.format(database=self.database, table_name=self.table_name, transformation_ctx=self.transformation_ctx)
        print(comment + sql)
        return self.transformation_ctx, comment + sql


class SQLServerDatasource(DatasourceInterface):
    def __init__(self, database=None, table_name=None):
        """
        :param database: database
        :param table_name: table_name
        """
        self.database = database
        self.table_name = table_name
        self.transformation_ctx = "DimUserSourceNode_node{random_id}".format(
                                                                        random_id=random.randint(1000000000001,
                                                                                                 1999999999999))

    def create_dynamic_frame(self):
        comment = "# Script generated for node DimUserSourceNode\n"
        sql = '''{transformation_ctx} = glueContext.create_dynamic_frame.from_catalog(
            database="{database}",
            table_name="{table_name}",
            transformation_ctx="{transformation_ctx}",
)'''.format(database=self.database, table_name=self.table_name, transformation_ctx=self.transformation_ctx)
        print(comment + sql)
        return self.transformation_ctx, comment + sql


class AmazonDynamoDatasource(DatasourceInterface):
    def create_dynamic_frame(self):
        pass

class ParquetDatasource(DatasourceInterface):
    def __init__(self, source_path=None):
        """
        :param source_path: source_path
        """

        self.source_path = source_path
        self.transformation_ctx = "S3bucket_node{random_id}".format(
            random_id=random.randint(1000000000001,
                                     1999999999999))

    def create_dynamic_frame(self):
        comment = "# Script generated for node parquet\n"
        sql = '''{transformation_ctx} = glueContext.create_dynamic_frame.from_options(
                   format_options={{}},
                   connection_type="s3",
                   format="parquet",
                   connection_options={{
                       "paths": ["{source_path}"],
                       "recurse": True,
                   }},
                   transformation_ctx="{transformation_ctx}",
        )'''.format(source_path=self.source_path, transformation_ctx=self.transformation_ctx)
        print(comment + sql)
        return self.transformation_ctx, comment + sql

class JsonDatasource(DatasourceInterface):
    def __init__(self, source_path=None):
        """
        :param source_path: source_path
        """

        self.source_path = source_path
        self.transformation_ctx = "S3bucket_node{random_id}".format(
            random_id=random.randint(1000000000001,
                                     1999999999999))

    def create_dynamic_frame(self):
        comment = "# Script generated for node json\n"
        sql = '''{transformation_ctx} = glueContext.create_dynamic_frame.from_options(
                   format_options={{"multiline": False}},
                   connection_type="s3",
                   format="json",
                   connection_options={{
                       "paths": ["{source_path}"],
                       "recurse": True,
                   }},
                   transformation_ctx="{transformation_ctx}",
        )'''.format(source_path=self.source_path, transformation_ctx=self.transformation_ctx)
        print(comment + sql)
        return self.transformation_ctx, comment + sql

class PgsqlMysqlDatasource(DatasourceInterface):
    def __init__(self, table_name=None):
        """
        :param source_path: source_path
        """
        self.table_name = table_name
        self.transformation_ctx = "DB_node{random_id}".format(
            random_id=random.randint(1000000000001,
                                     1999999999999))
    def create_dynamic_frame(self):
        comment = "# Script generated for node DB\n"

        sql = '''{transformation_ctx} = glueContext.create_dynamic_frame.from_options(
                connection_type=connection_type,
                connection_options={{
                    "useConnectionProperties": "true",
                    "dbtable":{table_name},
                    "connectionName":database
                }},
                transformation_ctx="{transformation_ctx}",
    )'''.format(table_name=self.table_name, transformation_ctx=self.transformation_ctx)
        print(comment + sql)
        return self.transformation_ctx, comment + sql

def generate_datasource_interface(datasource_type):
    return datasource_type.create_dynamic_frame()




#调用方法
# source_ctx, source = generate_datasource_interface(CsvDatasource(database='devops', table_name='user_csv'))
#调用 Sqlserver Datasource
#source_ctx, source = generate_datasource_interface(SQLServerDatasource(database='devops', table_name='cedc_dbo_dimuser'))
#调用 PostgreSQL Datasource
#source_ctx, source = generate_datasource_interface(PostgreSQLDatasource(database='postgresql_Elaine', table_name='dim_dag'))
#调用 Parquet Datasource
#source_ctx, source = generate_datasource_interface(ParquetDatasource(source_path='s3://eliane-bucket/output/Test1.csv'))
#调用 JSON Datasource
#source_ctx, source = generate_datasource_interface(JsonDatasource(source_path='s3://eliane-bucket/output/Test1.csv'))
#调用CSV Datasource
#source_ctx, source = generate_datasource_interface(CsvDatasource(quotechar='"', separator=",", source_path='s3://eliane-bucket/output/Test1.csv',withHeader=True))
#调用 PostgreSQL Datasource
# source = generate_datasource_interface(PgsqlMysqlDatasource(table_name='dim_dag'))
