# Databricks notebook source
# MAGIC %md
# MAGIC ![logo](https://cedc-databricks.s3.ap-northeast-1.amazonaws.com/images/cedc-logo-small.png)
# MAGIC # Load data from AWS S3(csv) to Delta Lake
# MAGIC
# MAGIC This notebook shows you how to import data from AWS S3 into a Delta Lake table using Python.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Choose Running Logic
# MAGIC #### run_mode: default is dev
# MAGIC - dev: testing purpose
# MAGIC - prod: airflow trigger mode, that means airflow will input the variables <br>
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- set default metastore
# MAGIC USE CATALOG main;
# MAGIC USE DATABASE powerbi;
# MAGIC SELECT current_database(), current_catalog();

# COMMAND ----------

# MAGIC %python
# MAGIC %pip install boto3
# MAGIC
# MAGIC run_mode = 'dev'

# COMMAND ----------

# MAGIC %python
# MAGIC import boto3
# MAGIC
# MAGIC if run_mode == 'prod':
# MAGIC     bucket_name = dbutils.widgets.get(bucket_name)
# MAGIC     s3_path = dbutils.widgets.get(s3_path)
# MAGIC elif run_mode == 'dev':
# MAGIC     bucket_name = 'cedc-databricks'
# MAGIC     s3_path = "data/"
# MAGIC     s3 = boto3.client('s3')
# MAGIC     response = s3.head_object(Bucket=bucket_name, Key=s3_path)
# MAGIC     # 检查Content-Type标头以判断对象类型
# MAGIC     content_type = response['ContentType']
# MAGIC     if content_type == 'application/x-directory':
# MAGIC         print(f'{path} is a directory')
# MAGIC     elif content_type == 'text/csv':
# MAGIC         print(f'{path} is a CSV file')
# MAGIC     else:
# MAGIC         print(f'{path} is neither a directory nor a CSV file')
# MAGIC else:
# MAGIC     print('Wrong run_mode, please input dev/prod to run the notebook.')
# MAGIC
# MAGIC
# MAGIC
# MAGIC     
# MAGIC

# COMMAND ----------

import requests

# 指定S3桶的URL和文件路径
s3_url = f"https://s3.amazonaws.com/{bucket_name}"
file_path = "/path/to/file.csv"

# 发送HTTP GET请求并获取响应
response = requests.get(s3_url + file_path)

# 将响应内容读取为字符串
data = response.content.decode('utf-8')

# 将字符串转换为Spark DataFrame
df = spark.read.csv(sc.parallelize([data]), header=True, inferSchema=True)

# 添加新列
remote_table_df = remote_table_df.withColumn("created_by", lit("system"))
remote_table_df = remote_table_df.withColumn("create_date", current_timestamp())

# 将数据写入Delta表
remote_table_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("table_name")

# 将表的元数据更新为使用远程表的结构
remote_table_schema = remote_table_df.schema.json()
spark.sql(f"ALTER TABLE table_name SET SERDEPROPERTIES ('schema'='{remote_table_schema}')")
