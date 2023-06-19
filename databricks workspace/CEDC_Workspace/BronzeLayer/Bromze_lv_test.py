# Databricks notebook source
# MAGIC %sql
# MAGIC -- set default metastore
# MAGIC USE CATALOG main;
# MAGIC USE DATABASE powerbi;
# MAGIC SELECT current_database(), current_catalog();

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC DBFS 中默认的存储位置称为 DBFS 根（root），以下 DBFS 根位置中存储了几种类型的数据：  
# MAGIC
# MAGIC /FileStore：导入的数据文件、生成的绘图以及上传的库  
# MAGIC /databricks-datasets：示例公共数据集，用于学习Spark或者测试算法。  
# MAGIC /databricks-results：通过下载查询的完整结果生成的文件。  
# MAGIC /tmp：存储临时数据的目录  
# MAGIC /user：存储各个用户的文件  
# MAGIC /mnt：（默认是不可见的）装载（挂载）到DBFS的文件，写入装载点路径(/mnt)中的数据存储在DBFS根目录之外。  
# MAGIC
# MAGIC URL:abfss://cedc-unity@cedcexternalstorage.dfs.core.windows.net/mnt  
# MAGIC main_metastore_managed_table_external_location  
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore"))

dbutils.fs.help()

 display(dbutils.fs.ls("dbfs:/FileStore/test_data_source"))

# COMMAND ----------

# MAGIC %md
# MAGIC Syntax for schema inference and evolution
# MAGIC Specifying a target directory for the option cloudFiles.schemaLocation enables schema inference and evolution. You can choose to use the same directory you specify for the checkpointLocation. If you use Delta Live Tables, Azure Databricks manages schema location and other checkpoint information automatically.

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

@dlt.table
def bronze_fct_dlt_demo():
    source='dbfs:/FileStore/test_data_source'

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{source}/fct_dlt_demo")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )

# COMMAND ----------

import dlt

json_path = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-input-dataset>"
@dlt.create_table(
  comment="Data ingested from an ADLS2 storage account."
)
def read_from_ADLS2():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .load(json_path)
  )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 以S3为数据源的情况  
# MAGIC

# COMMAND ----------

bucket_name = 'cedc-databricks'
    s3_path = "data/"
    s3 = boto3.client('s3')
    response = s3.head_object(Bucket=bucket_name, Key=s3_path)
    # 检查Content-Type标头以判断对象类型
    content_type = response['ContentType']
    if content_type == 'application/x-directory':
        print(f'{path} is a directory')
    elif content_type == 'text/csv':
        print(f'{path} is a CSV file')
    else:
        print(f'{path} is neither a directory nor a CSV file')

# COMMAND ----------

# MAGIC %md
# MAGIC https://www.databricks.com/blog/2022/09/29/using-streaming-delta-live-tables-and-aws-dms-change-data-capture-mysql.html
# MAGIC
# MAGIC https://www.databricks.com/blog/2018/10/29/simplifying-change-data-capture-with-databricks-delta.html
# MAGIC
