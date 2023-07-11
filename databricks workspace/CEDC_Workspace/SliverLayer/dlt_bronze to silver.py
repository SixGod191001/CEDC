# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog main;
# MAGIC use database powerbi;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ext_test

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe extended ext_test

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

params={"homepath":'abfss://cedc-unity@cedcexternalstorage.dfs.core.windows.net/mnt/',
        "object":'orders',
        "format":"delta"
        "file_format":"json",
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
    if params['format'] == 'cloudFiles'
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

def readstream_cloudFiles():
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
def readstream_delta():
    return (
        spark.readStream
            .format("delta")
            .load(f"{homepath}/{filename}")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

class process_silver():
    """
    default_param = 
    {   "homepath":'abfss://cedc-unity@cedcexternalstorage.dfs.core.windows.net/mnt/',
        "object":'orders',
        "format":"delta"
        "file_format":"json",
        "source_table":'orders-bronze',
        "target_table"：'orders-silver',
        "filename":"test"
             }
    """
    source_table='dlt_test'
    def __init__(self,param):
        #initial parameter
        source = spark.conf.get("source")
        for k,v in param.items():
            setattr(self,k,v)
  
    
    def readstream_cloudFiles(self):
        return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", self.file_format)
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{self.homepath}/{self.filename}")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
                   )
              )
    def readstream_delta(self):
        return (
        spark.readStream
            .format("delta")
            .table(self.source_table)
            .select(
                F.current_timestamp().alias("processing_time"), 
                "*"
                   )
               )
    def process(self):
        self.dlt_bronze()
    
    @dlt.table(
        name= source_table,
        comment = "Append into target table",
        table_properties = {"quality": "bronze"})
    def dlt_bronze(self):
        if self.format == 'delta':
            self.readstream_delta()


# COMMAND ----------


