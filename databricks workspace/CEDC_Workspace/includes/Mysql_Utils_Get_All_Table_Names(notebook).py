# Databricks notebook source
# MAGIC %md
# MAGIC ![logo](https://cedc-databricks.s3.ap-northeast-1.amazonaws.com/images/cedc-logo-small.png)
# MAGIC ## Common Function - Get all table names from MYSQL database

# COMMAND ----------

# MAGIC %md
# MAGIC ### Init
# MAGIC Get variables from caller

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC host = dbutils.widgets.get("host")
# MAGIC port = dbutils.widgets.get("port")
# MAGIC database = dbutils.widgets.get("database")
# MAGIC user = dbutils.widgets.get("user")
# MAGIC password = dbutils.widgets.get("password")

# COMMAND ----------

# MAGIC %md
# MAGIC #### For testing purpose 
# MAGIC You should uncomment below code and start from it.

# COMMAND ----------

# %python
# host = 'faracedc.mysql.database.azure.com'
# port = 3306
# database = 'apdb'
# user = 'fara'
# password = 'ZAQ12wsxcde#'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query MYSQL with jdbc

# COMMAND ----------

driver = "org.mariadb.jdbc.Driver"

table_names_df = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:mysql://{host}:{port}/{database}")
    .option("driver", driver)
    .option(
        "dbtable",
        f"(SELECT table_name FROM information_schema.tables WHERE table_schema = '{database}') AS tables",
    )
    .option("user", user)
    .option("password", password)
    .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Return result to caller

# COMMAND ----------

table_names = [row["table_name"] for row in table_names_df.collect()]
print(table_names)
dbutils.notebook.exit(table_names)
