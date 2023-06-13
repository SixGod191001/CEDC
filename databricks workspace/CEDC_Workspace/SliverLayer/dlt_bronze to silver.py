# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ly_location;
# MAGIC

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

params={"homepath":'dbfs:/user/hive/warehouse/',
        "object":'orders',
        "format":"json",
        "source_table":'orders-bronze',
        "target_table"：'orders-silver'
}
homepath=params['homepath']
filename=params['object']
file_format=params['format']
bronze_table=params['source_table']
silver_table=params['target_table']

@dlt.table(
    name= bronze_table,
    comment = "Append into target table",
    table_properties = {"quality": "silver"})
def dlt_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", file_format)
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{homepath}/{filename}")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )

# COMMAND ----------

@dlt.table(
    name= silver_table,
    comment = "Append into target table",
    table_properties = {"quality": "silver"})
# @dlt.expect_or_fail("valid_date", F.col("order_timestamp") > "2021-01-01")
def dlt_silver_append():
    return (
bronze_table=dlt.read_stream(bronze_table)
            .select(
                "*"
            )
    )

# COMMAND ----------


