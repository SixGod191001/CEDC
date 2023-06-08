# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Load data from MySQL to Delta Lake
# MAGIC
# MAGIC This notebook shows you how to import data from MySQL databases into a Delta Lake table using Python.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Init Variables
# MAGIC There are two type run_mode.
# MAGIC - dev: testing purpose
# MAGIC - prod: airflow trigger mode, that means airflow will input the variables

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.utils import AnalysisException
# MAGIC
# MAGIC def get_notebook_info():
# MAGIC     try:
# MAGIC         # 获取当前notebook的路径
# MAGIC         notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# MAGIC
# MAGIC         # 获取当前工程的路径
# MAGIC         project_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().extraContext().get("project_path")
# MAGIC
# MAGIC         # 获取当前用户的用户名
# MAGIC         username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply("user")
# MAGIC
# MAGIC         return {"notebook_path": notebook_path, "project_path": project_path, "username": username}
# MAGIC     except AnalysisException:
# MAGIC         return {"notebook_path": None, "project_path": None, "username": None}
# MAGIC get_notebook_info()

# COMMAND ----------

# MAGIC %python
# MAGIC run_mode = 'dev'

# COMMAND ----------

# MAGIC %python
# MAGIC if run_mode == 'prod':
# MAGIC     dbtable = dbutils.widgets.get("dbtable")
# MAGIC     host = dbutils.widgets.get("host")
# MAGIC     port = dbutils.widgets.get("port")
# MAGIC     database = dbutils.widgets.get("database")
# MAGIC     user = dbutils.widgets.get("user")
# MAGIC     password = dbutils.widgets.get("password")
# MAGIC elif run_mode == 'dev':
# MAGIC     %python
# MAGIC     dbtable = 'dim_geography'
# MAGIC     host = 'faracedc.mysql.database.azure.com'
# MAGIC     port = 3306
# MAGIC     database = 'apdb'
# MAGIC     user = 'fara'
# MAGIC     password = 'ZAQ12wsxcde#'
# MAGIC else:
# MAGIC     print('Wrong run_mode, please input dev/prod to run the notebook.')
# MAGIC
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Call the Utils function 
# MAGIC   - Get table schema from souce table
# MAGIC   - Based on the schema to ceate delta table if table not exists

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC dbutils.notebook.run(
# MAGIC     "/Users/class+031@databricks.com/utils/utils_read_mysql_table",
# MAGIC     60,
# MAGIC     {
# MAGIC         "dbtable": dbtable,
# MAGIC         "host": host,
# MAGIC         "port": port,
# MAGIC         "database": database,
# MAGIC         "user": user,
# MAGIC         "password": password,
# MAGIC     },
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Generate create table DDL by input json structs

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;
