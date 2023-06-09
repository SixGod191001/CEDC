# Databricks notebook source
# MAGIC %md
# MAGIC #nb_s2g_fact_ims_city

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recreate table ``g_fact_ims_city``

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists g_fact_ims_city;
# MAGIC create table if not exists g_fact_ims_city as 
# MAGIC select
# MAGIC s.YM,
# MAGIC s.CityCode,
# MAGIC s.IMSCITY_Amount,
# MAGIC s.IMSCITY_QTY
# MAGIC from s_fact_ims_city s 
# MAGIC where 1=2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert data into table ``g_fact_ims_city``

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into g_fact_ims_city
# MAGIC select
# MAGIC s.YM,
# MAGIC s.CityCode,
# MAGIC sum(s.IMSCITY_Amount),
# MAGIC sum(s.IMSCITY_QTY)
# MAGIC from s_fact_ims_city s 
# MAGIC group by s.YM,
# MAGIC s.CityCode;
