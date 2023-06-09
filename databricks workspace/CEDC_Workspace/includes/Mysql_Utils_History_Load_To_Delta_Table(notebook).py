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


remote_table_df = (
    spark.read.format("mysql")
    .option("dbtable", dbtable)
    .option("host", host)
    .option("port", port)
    .option("database", database)
    .option("user", user)
    .option("password", password)
    .load()
)
remote_table_df.write.mode("overwrite").saveAsTable(target_table_name)
