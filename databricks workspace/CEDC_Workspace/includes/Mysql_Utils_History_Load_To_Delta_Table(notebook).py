# Databricks notebook source
# MAGIC %md
# MAGIC ![logo](https://cedc-databricks.s3.ap-northeast-1.amazonaws.com/images/cedc-logo-small.png)
# MAGIC
# MAGIC ## Common Function - History Load Data From Mysql To Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Init Variables
# MAGIC Get Variables From Callers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- set default metastore
# MAGIC USE CATALOG main;
# MAGIC USE DATABASE powerbi;
# MAGIC SELECT current_database(), current_catalog();

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC dbtable = dbutils.widgets.get("dbtable")
# MAGIC host = dbutils.widgets.get("host")
# MAGIC port = dbutils.widgets.get("port")
# MAGIC database = dbutils.widgets.get("database")
# MAGIC user = dbutils.widgets.get("user")
# MAGIC password = dbutils.widgets.get("password")
# MAGIC target_table_name = dbutils.widgets.get("target_table_name")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start History Load

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import lit, current_timestamp
# MAGIC
# MAGIC remote_table_df = (
# MAGIC     spark.read.format("mysql")
# MAGIC     .option("dbtable", dbtable)
# MAGIC     .option("host", host)
# MAGIC     .option("port", port)
# MAGIC     .option("database", database)
# MAGIC     .option("user", user)
# MAGIC     .option("password", password)
# MAGIC     .load()
# MAGIC )
# MAGIC
# MAGIC # 添加新列
# MAGIC remote_table_df = remote_table_df.withColumn("created_by", lit("system"))
# MAGIC remote_table_df = remote_table_df.withColumn("create_date", current_timestamp())
# MAGIC
# MAGIC remote_table_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table_name)
