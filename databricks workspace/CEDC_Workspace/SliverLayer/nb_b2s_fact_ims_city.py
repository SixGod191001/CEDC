# Databricks notebook source
# MAGIC %md
# MAGIC ### Create table ``s_fact_ims_city``

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists s_fact_ims_city as 
# MAGIC select
# MAGIC b.YM,
# MAGIC b.CityCode,
# MAGIC b.BrandCode,
# MAGIC b.Corp,
# MAGIC b.Product,
# MAGIC b.Molecule,
# MAGIC b.IMSCITY_Amount,
# MAGIC b.IMSCITY_QTY,
# MAGIC b.Defined_MKT,
# MAGIC b.FocusCompeGrp,
# MAGIC b.Manufactory,
# MAGIC b.Flag_Aspen,
# MAGIC b.PACKAGE,
# MAGIC b.PACK_SIZE,
# MAGIC b.NFCI_Code,
# MAGIC b.NFCI_Description,
# MAGIC bb.BrandDesc
# MAGIC from b_fact_ims_city b 
# MAGIC JOIN b_dim_brand bb
# MAGIC ON b.brandcode=bb.brandcode
# MAGIC where 1=2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incrementally insert data into table ``s_fact_ims_city``

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO s_fact_ims_city s
# MAGIC USING b_fact_ims_city b
# MAGIC ON s.ym=b.ym
# MAGIC AND s.citycode=b.citycode
# MAGIC AND s.brandcode=b.brandcode
# MAGIC AND s.defined_mkt=b.defined_mkt
# MAGIC AND s.product=b.product
# MAGIC JOIN b_dim_brand bb
# MAGIC ON b.brandcode=bb.brandcode
# MAGIC WHERE b.ym>202302
# MAGIC WHEN MATCHED THEN UPDATE SET s.corp=b.corp
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from b_fact_ims_city
# MAGIC -- select count(*),YM,CityCode,BrandCode,Defined_MKT,PRODUCT from b_fact_ims_city
# MAGIC -- group by YM,CityCode,BrandCode,Defined_MKT,PRODUCT
# MAGIC -- having COUNT(*) >1
# MAGIC
# MAGIC -- SELECT * FROM b_fact_ims_city
# MAGIC -- WHERE YM=202201
# MAGIC -- AND CITYCODE=532800
# MAGIC -- AND BRANDCODE='Brand Code 002'
# MAGIC -- AND DEFINED_MKT='Pay-per-click (PPC) advertising'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from b_dim_brand

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from b_dim_geography

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table s_fact_ims_city
