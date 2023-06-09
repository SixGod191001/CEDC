# Databricks notebook source
# MAGIC %md
# MAGIC #nb_b2s_fact_ims_city

# COMMAND ----------

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
# MAGIC USING (
# MAGIC     select bf.YM,
# MAGIC     bf.CityCode,
# MAGIC     bf.BrandCode,
# MAGIC     bf.Corp,
# MAGIC     bf.Product,
# MAGIC     bf.Molecule,
# MAGIC     bf.IMSCITY_Amount,
# MAGIC     bf.IMSCITY_QTY,
# MAGIC     bf.Defined_MKT,
# MAGIC     bf.FocusCompeGrp,
# MAGIC     bf.Manufactory,
# MAGIC     bf.Flag_Aspen,
# MAGIC     bf.PACKAGE,
# MAGIC     bf.PACK_SIZE,
# MAGIC     bf.NFCI_Code,
# MAGIC     bf.NFCI_Description,
# MAGIC     bb.BrandDesc
# MAGIC     from b_fact_ims_city bf
# MAGIC     JOIN b_dim_brand bb
# MAGIC     ON bf.brandcode=bb.brandcode
# MAGIC     WHERE bf.ym>202302) as b
# MAGIC ON s.ym=b.ym
# MAGIC AND s.citycode=b.citycode
# MAGIC AND s.brandcode=b.brandcode
# MAGIC AND s.defined_mkt=b.defined_mkt
# MAGIC AND s.product=b.product
# MAGIC WHEN MATCHED THEN UPDATE SET 
# MAGIC     s.YM                =b.YM,
# MAGIC     s.CityCode          =b.CityCode,
# MAGIC     s.BrandCode         =b.BrandCode,
# MAGIC     s.Corp              =b.Corp,
# MAGIC     s.Product           =b.Product,
# MAGIC     s.Molecule          =b.Molecule,
# MAGIC     s.IMSCITY_Amount    =b.IMSCITY_Amount,
# MAGIC     s.IMSCITY_QTY       =b.IMSCITY_QTY,
# MAGIC     s.Defined_MKT       =b.Defined_MKT,
# MAGIC     s.FocusCompeGrp     =b.FocusCompeGrp,
# MAGIC     s.Manufactory       =b.Manufactory,
# MAGIC     s.Flag_Aspen        =b.Flag_Aspen,
# MAGIC     s.PACKAGE           =b.PACKAGE,
# MAGIC     s.PACK_SIZE         =b.PACK_SIZE,
# MAGIC     s.NFCI_Code         =b.NFCI_Code,
# MAGIC     s.NFCI_Description  =b.NFCI_Description,
# MAGIC     s.BrandDesc         =b.BrandDesc
# MAGIC WHEN NOT MATCHED THEN INSERT *
