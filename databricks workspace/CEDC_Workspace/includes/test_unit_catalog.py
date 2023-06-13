# Databricks notebook source
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
