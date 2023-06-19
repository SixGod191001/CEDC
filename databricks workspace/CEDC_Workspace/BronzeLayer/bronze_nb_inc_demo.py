# Databricks notebook source
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
