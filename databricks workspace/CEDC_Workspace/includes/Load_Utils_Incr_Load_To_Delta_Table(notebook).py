# Databricks notebook source
# MAGIC %md
# MAGIC ![logo](https://cedc-databricks.s3.ap-northeast-1.amazonaws.com/images/cedc-logo-small.png)
# MAGIC ## Common Function - Incremental Load Data to Delta Table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE test_table (
# MAGIC   id INT,
# MAGIC   date STRING,
# MAGIC   value INT
# MAGIC )
# MAGIC USING delta

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls("/root/")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC def merge_incremental_data(source_df, target_table_name, primary_keys):
# MAGIC     """
# MAGIC     :source_df: source table dataframe (dataframe)
# MAGIC     :target_table_name: target table name (string)
# MAGIC     :target_table_path: target table path (string)
# MAGIC     :primary_keys: list of primary keys (list)
# MAGIC     """
# MAGIC     print(f"delta.`{target_table_name}`")
# MAGIC     # 读取目标表的数据
# MAGIC     target_table = spark.read.format("delta").load(f"delta.`{target_table_name}`")
# MAGIC
# MAGIC     # 构建merge条件
# MAGIC     merge_condition = " and ".join([f"source_df.{key} = target_table.{key}" for key in primary_keys])
# MAGIC
# MAGIC     # 执行merge操作
# MAGIC     merged_data = target_table \
# MAGIC         .merge(source_df, merge_condition, "outer") \
# MAGIC         .select("source_df.*") \
# MAGIC         .where(col(primary_keys[0]).isNotNull())
# MAGIC     merged_data.write.format("delta").mode("append").save(f"delta.`{target_table_name}`")
# MAGIC
# MAGIC # 示例：将source_df的数据与target_table中的数据进行合并，根据id和date两列进行比较
# MAGIC source_df = spark.createDataFrame([(1, "2022-01-01", 100), (2, "2022-01-02", 200)], ["id", "date", "value"])
# MAGIC target_table_name = "test_table"
# MAGIC primary_keys = ["id", "date"]
# MAGIC merge_incremental_data(source_df, target_table_name, primary_keys)
