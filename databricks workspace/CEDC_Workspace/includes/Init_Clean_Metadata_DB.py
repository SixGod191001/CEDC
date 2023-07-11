# Databricks notebook source
# MAGIC %md
# MAGIC ![logo](https://cedc-databricks.s3.ap-northeast-1.amazonaws.com/images/cedc-logo-small.png)
# MAGIC # Clean Metadata DB
# MAGIC Note that using this script, it will empty all tables in the metadata, as well as related data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- set default metastore
# MAGIC USE CATALOG main;
# MAGIC USE DATABASE powerbi;
# MAGIC SELECT current_database(), current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database(), current_catalog();

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC # 获取所有表
# MAGIC tables = spark.sql("SHOW TABLES").collect()
# MAGIC
# MAGIC # 删除每个表
# MAGIC for table in tables:
# MAGIC     spark.sql(f"DROP TABLE IF EXISTS {table.tableName}")
# MAGIC
# MAGIC # 关闭SparkSession
# MAGIC # spark.stop()
