# Databricks notebook source
# MAGIC %md
# MAGIC ![logo](https://cedc-databricks.s3.ap-northeast-1.amazonaws.com/images/cedc-logo-small.png)
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
# MAGIC
# MAGIC import json
# MAGIC
# MAGIC notebook_info = json.loads(
# MAGIC     dbutils.notebook.run("../utils/get_project_info", timeout_seconds=60)
# MAGIC )
# MAGIC print(notebook_info)
# MAGIC print(type(notebook_info))

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC run_mode = 'dev'

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC if run_mode == 'prod':
# MAGIC     dbtable = dbutils.widgets.get("dbtable")
# MAGIC     host = dbutils.widgets.get("host")
# MAGIC     port = dbutils.widgets.get("port")
# MAGIC     database = dbutils.widgets.get("database")
# MAGIC     user = dbutils.widgets.get("user")
# MAGIC     password = dbutils.widgets.get("password")
# MAGIC elif run_mode == 'dev':
# MAGIC     dbtable = 'dim_calendar'
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
# MAGIC     f"{notebook_info['utils_path']}/create_delta_table_by_mysql_schema",
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
# MAGIC #### Check if table exists

# COMMAND ----------

# MAGIC %python
# MAGIC res_df = spark.sql(f"DESCRIBE EXTENDED {dbtable}")
# MAGIC
# MAGIC
# MAGIC
