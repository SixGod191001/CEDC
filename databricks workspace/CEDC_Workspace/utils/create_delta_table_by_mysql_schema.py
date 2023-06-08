# Databricks notebook source
# MAGIC %md
# MAGIC ## Get Variables from caller

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC dbtable = dbutils.widgets.get("dbtable")
# MAGIC host = dbutils.widgets.get("host")
# MAGIC port = dbutils.widgets.get("port")
# MAGIC database = dbutils.widgets.get("database")
# MAGIC user = dbutils.widgets.get("user")
# MAGIC password = dbutils.widgets.get("password")
# MAGIC
# MAGIC print('dbtable is: {}'.format(dbtable))
# MAGIC print('host is: {}'.format(host))
# MAGIC print('port is: {}'.format(port))
# MAGIC print('database is: {}'.format(database))
# MAGIC print('user is: {}'.format(user))
# MAGIC print('password is: {}'.format(password))
# MAGIC

# COMMAND ----------

'''%python
dbtable = 'dim_org'
host = 'faracedc.mysql.database.azure.com'
port = 3306
database = 'aspen'
user = 'fara'
password = 'ZAQ12wsxcde#'''

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function: Connect to mysql table

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC def read_remote_table(dbtable, host, port, database, user, password):
# MAGIC     remote_table_df = (spark.read
# MAGIC       .format("mysql")
# MAGIC       .option("dbtable", dbtable)
# MAGIC       .option("host", host)
# MAGIC       .option("port", port)
# MAGIC       .option("database", database)
# MAGIC       .option("user", user)
# MAGIC       .option("password", password)
# MAGIC       .load()
# MAGIC     )
# MAGIC     return remote_table_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute the read_remote_table function

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC remote_table_df = read_remote_table(dbtable, host, port, database, user, password)
# MAGIC display(remote_table_df)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function: Create Delta table base on the schema from Mysql

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.types import *
# MAGIC
# MAGIC def spark_type_to_sql_type(spark_type):
# MAGIC     if isinstance(spark_type, StringType):
# MAGIC         return "STRING"
# MAGIC     elif isinstance(spark_type, IntegerType):
# MAGIC         return "INT"
# MAGIC     elif isinstance(spark_type, LongType):
# MAGIC         return "BIGINT"
# MAGIC     elif isinstance(spark_type, DoubleType):
# MAGIC         return "DOUBLE"
# MAGIC     elif isinstance(spark_type, FloatType):
# MAGIC         return "FLOAT"
# MAGIC     elif isinstance(spark_type, DecimalType):
# MAGIC         return f"DECIMAL({spark_type.precision},{spark_type.scale})"
# MAGIC     elif isinstance(spark_type, TimestampType):
# MAGIC         return "TIMESTAMP"
# MAGIC     elif isinstance(spark_type, DateType):
# MAGIC         return "DATE"
# MAGIC     elif isinstance(spark_type, BooleanType):
# MAGIC         return "BOOLEAN"
# MAGIC     elif isinstance(spark_type, BinaryType):
# MAGIC         return "BINARY"
# MAGIC     elif isinstance(spark_type, ArrayType):
# MAGIC         return f"ARRAY<{spark_type_to_sql_type(spark_type.elementType)}>"
# MAGIC     elif isinstance(spark_type, MapType):
# MAGIC         return f"MAP<{spark_type_to_sql_type(spark_type.keyType)},{spark_type_to_sql_type(spark_type.valueType)}>"
# MAGIC     elif isinstance(spark_type, StructType):
# MAGIC         return "STRUCT"
# MAGIC     else:
# MAGIC         raise ValueError(f"Unsupported data type: {spark_type}")
# MAGIC
# MAGIC def create_delta_table(df, table_name):
# MAGIC     # 获取DataFrame的schema
# MAGIC     schema = df.schema
# MAGIC
# MAGIC     # 构建表定义字符串
# MAGIC     table_definition = ',\n'.join([f'    {field.name} {spark_type_to_sql_type(field.dataType)}' for field in schema])
# MAGIC
# MAGIC     # 构建CREATE TABLE语句
# MAGIC     create_table_stmt = f"""
# MAGIC     CREATE TABLE IF NOT EXISTS {table_name}
# MAGIC     (
# MAGIC     {table_definition}
# MAGIC     ) USING DELTA;
# MAGIC     """
# MAGIC
# MAGIC     # 执行CREATE TABLE语句
# MAGIC     try:
# MAGIC         spark.sql(create_table_stmt)
# MAGIC         print(f"Table {table_name} created successfully!")
# MAGIC     except AnalysisException as e:
# MAGIC         print(f"Error creating table {table_name}: {e}")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Call the create_delta_table function

# COMMAND ----------

# MAGIC %python
# MAGIC # 使用示例
# MAGIC create_delta_table(remote_table_df, dbtable)
