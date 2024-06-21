# COMMAND ----------

# MAGIC %sql
# MAGIC -- set default metastore
# MAGIC USE CATALOG main;
# MAGIC USE DATABASE powerbi;
# MAGIC SELECT current_database(), current_catalog();