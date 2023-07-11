# Databricks notebook source
pip install databricks_api
from databricks_api import DatabricksAPI

db = DatabricksAPI(host='https://adb-7887939471123750.10.azuredatabricks.net/', token='dapi356cde2118ce3353c7e4fe38dcd29046-2')

db.sql('USE CATALOG main')
# db.sql('GRANT SELECT ON TABLE mytable TO user1')

# COMMAND ----------

df = spark.createDataFrame([(1, "John"), (2, "Jane"), (3, "Bob")], ["id", "name"])


# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("testtable1")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database(), current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG main;
# MAGIC USE DATABASE powerbi;
# MAGIC SELECT current_database(), current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS main.powerbi.test_table
# MAGIC (
# MAGIC   deptcode   INT,
# MAGIC   deptname  STRING
# MAGIC );

# COMMAND ----------

dbutils.FSHandler.ls?
