# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG main;
# MAGIC USE DATABASE powerbi;

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/test_data_source/"))

display(dbutils.fs.ls("/FileStore/test_data_source/test_sales_data/"))

display(dbutils.fs.ls("abfss://cedc-unity@cedcexternalstorage.dfs.core.windows.net/mnt/test_data_source"))

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

@dlt.table
def bronze_fct_inc_demo():
    source='dbfs:/FileStore'
    tmp_path='dbfs:/tmp'
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            # .option("cloudFiles.inferColumnTypes", True)
            .option("cloudFiles.schemaLocation", tmp_path)
            .load(f"{source}/inc_data")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE SCHEMA EXTENDED main.powerbi;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * AS create_date from temp_delays_all;

# COMMAND ----------

query_df = spark.sql(f"SELECT *,current_timestamp() AS create_date FROM temp_delays_all")
# display(query_df)
# query_df.write.format("csv").save("/FileStore/test_data_source/test_sales_data2.csv")
query_df.write.format("csv").mode('append').save("/FileStore/test_data_source/inc_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
# MAGIC --   path = '/FileStore/test_data_source/test_sales_data.csv',
# MAGIC --   header = "true",
# MAGIC --   mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
# MAGIC -- );
# MAGIC
# MAGIC -- CREATE OR REPLACE TEMPORARY VIEW temp_delays_all USING CSV OPTIONS (
# MAGIC --   path = '/FileStore/test_data_source/test_sales_data',
# MAGIC --   header = "true",
# MAGIC --   mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
# MAGIC -- );
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_delays_inc USING CSV OPTIONS (
# MAGIC   path = '/FileStore/test_data_source/inc_data',
# MAGIC   header = "true",
# MAGIC   mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create a new external Unity Catalog table from an existing table
# MAGIC -- Replace <bucket_path> with the storage location where the table will be created
# MAGIC -- CREATE TABLE IF NOT EXISTS temp_use4insert_demo
# MAGIC -- LOCATION '/FileStore/test_data_source'
# MAGIC -- AS SELECT * from temp_delays;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS trips_external
# MAGIC LOCATION 'abfss://cedc-unity@cedcexternalstorage.dfs.core.windows.net/mnt/test_data_source'
# MAGIC AS SELECT * from samples.nyctaxi.trips;
# MAGIC  
# MAGIC -- To use a storage credential directly, add 'WITH (CREDENTIAL <credential_name>)' to the SQL statement.
# MAGIC
# MAGIC -- CREATE TABLE temp_use4insert_demo LOCATION '/FileStore/test_data_source2'
# MAGIC -- AS SELECT * FROM temp_delays;
# MAGIC
# MAGIC -- SELECT * FROM external_table; 
# MAGIC
# MAGIC -- CREATE TABLE temp_use4insert_demo USING CSV LOCATION '/FileStore/test_data_source'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from trips_external

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe trips_external

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

@dlt.table
def bronze_fct_trips_demo():
    source='abfss://cedc-unity@cedcexternalstorage.dfs.core.windows.net/mnt/test_data_source'
    tmp_path='dbfs:/tmp'
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            # .option("cloudFiles.inferColumnTypes", True)
            .option("cloudFiles.schemaLocation", tmp_path)
            .load(f"{source}")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC + 创建视图，引用3条测试数据  
# MAGIC + 从视图作为来源，加上时间列写入csv文件夹  
# MAGIC + csv文件夹作为源，建立dlt表  
# MAGIC + pipelin引用nb  
# MAGIC
# MAGIC "abfss://cedc-unity@cedcexternalstorage.dfs.core.windows.net/mnt/demo_inc_data"  
# MAGIC dbfs:/FileStore/test_data_source/test_sales_data.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW demo_inc_data USING CSV OPTIONS (
# MAGIC   path = 'dbfs:/FileStore/test_data_source/test_sales_data.csv',
# MAGIC   header = "true",
# MAGIC   mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
# MAGIC );

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from demo_inc_data

# COMMAND ----------

query_df = spark.sql(f"SELECT *,current_timestamp() AS create_date FROM demo_inc_data")
query_df.write.format("csv").mode('append').save("abfss://cedc-unity@cedcexternalstorage.dfs.core.windows.net/mnt/demo_inc_data")

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

@dlt.table
def bronze_fct_inc_demo():
    source='abfss://cedc-unity@cedcexternalstorage.dfs.core.windows.net/mnt/demo_inc_data'
    tmp_path='dbfs:/tmp'
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            # .option("cloudFiles.inferColumnTypes", True)
            .option("cloudFiles.schemaLocation", tmp_path)
            .load(source)
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )
