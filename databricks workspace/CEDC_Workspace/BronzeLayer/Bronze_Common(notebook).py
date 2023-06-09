# Databricks notebook source
# MAGIC %md
# MAGIC ![logo](https://cedc-databricks.s3.ap-northeast-1.amazonaws.com/images/cedc-logo-small.png)
# MAGIC # Load data from MySQL to Delta Lake
# MAGIC
# MAGIC This notebook shows you how to import data from MySQL databases into a Delta Lake table using Python.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Init Variables
# MAGIC Load Project information

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC import json
# MAGIC # load the project paths info
# MAGIC notebook_info = json.loads(
# MAGIC     dbutils.notebook.run("../includes/Init_Utils_Get_ProjectInfo(notebook)", timeout_seconds=60)
# MAGIC )
# MAGIC print(notebook_info)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Choose Running Logic
# MAGIC #### run_mode: default is dev
# MAGIC - dev: testing purpose
# MAGIC - prod: airflow trigger mode, that means airflow will input the variables <br>
# MAGIC #### load_mode: default is HISTORY
# MAGIC - INCR
# MAGIC - HISTORY
# MAGIC #### dbtable: default is all
# MAGIC - all
# MAGIC - table_name

# COMMAND ----------

# MAGIC %python
# MAGIC run_mode = 'dev'
# MAGIC load_mode = 'HISTORY'
# MAGIC

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
# MAGIC     dbtable = 'dim_brand'
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
# MAGIC #### Implement Delta Table Structure 
# MAGIC   - Get table schema from souce table
# MAGIC   - Based on the schema to ceate delta table if table not exists

# COMMAND ----------

# MAGIC %python
# MAGIC target_table_name = f"default.b_{dbtable}"

# COMMAND ----------

# MAGIC %python
# MAGIC if load_mode == "INCR":
# MAGIC     dbutils.notebook.run(
# MAGIC         f"{notebook_info['includes_path']}/Mysql_Utils_Create_Delta_Table_By_Schema(notebook)",
# MAGIC         60,
# MAGIC         {
# MAGIC             "dbtable": dbtable,
# MAGIC             "host": host,
# MAGIC             "port": port,
# MAGIC             "database": database,
# MAGIC             "user": user,
# MAGIC             "password": password,
# MAGIC         },
# MAGIC     )
# MAGIC elif load_mode == "HISTORY":
# MAGIC     if dbtable == "all":
# MAGIC         table_list = list(
# MAGIC             dbutils.notebook.run(
# MAGIC                 f"{notebook_info['includes_path']}/Mysql_Utils_Get_All_Table_Names(notebook)",
# MAGIC                 60,
# MAGIC                 {
# MAGIC                     "host": host,
# MAGIC                     "port": port,
# MAGIC                     "database": database,
# MAGIC                     "user": user,
# MAGIC                     "password": password,
# MAGIC                 },
# MAGIC             )
# MAGIC         )
# MAGIC         if len(table_list) == 0:
# MAGIC             print("Get zero table.")
# MAGIC         else:
# MAGIC             for t in table_list:
# MAGIC                 target_table_name = f"default.b_{t}"
# MAGIC                 dbutils.notebook.run(
# MAGIC                     f"{notebook_info['includes_path']}/Mysql_Utils_History_Load_To_Delta_Table(notebook)",
# MAGIC                     60,
# MAGIC                     {
# MAGIC                         "dbtable": t,
# MAGIC                         "host": host,
# MAGIC                         "port": port,
# MAGIC                         "database": database,
# MAGIC                         "user": user,
# MAGIC                         "password": password,
# MAGIC                         "target_table_name": target_table_name,
# MAGIC                     },
# MAGIC                 )
# MAGIC
# MAGIC     else:
# MAGIC         target_table_name = f"default.b_{dbtable}"
# MAGIC         dbutils.notebook.run(
# MAGIC             f"{notebook_info['includes_path']}/Mysql_Utils_History_Load_To_Delta_Table(notebook)",
# MAGIC             60,
# MAGIC             {
# MAGIC                 "dbtable": dbtable,
# MAGIC                 "host": host,
# MAGIC                 "port": port,
# MAGIC                 "database": database,
# MAGIC                 "user": user,
# MAGIC                 "password": password,
# MAGIC                 "target_table_name": target_table_name,
# MAGIC             },
# MAGIC         )
# MAGIC else:
# MAGIC     pass

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if table exists

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;
