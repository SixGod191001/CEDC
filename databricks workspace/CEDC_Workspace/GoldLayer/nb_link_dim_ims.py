# Databricks notebook source
# MAGIC %md
# MAGIC set up default catalog and database

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog main;
# MAGIC use database powerbi;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view temp_dim_main
# MAGIC as
# MAGIC select ym, citycode
# MAGIC from (
# MAGIC     select distinct ym, 1 as joinkey from b_fact_cpa
# MAGIC     union
# MAGIC     select distinct ym ,1 as joinkey from b_fact_ims_city
# MAGIC ) ym
# MAGIC left join (
# MAGIC     select citycode, 1 as joinkey from b_fact_ims_city
# MAGIC     union
# MAGIC     select 'NoIMSCity' as CityCode, 1 as joinkey 
# MAGIC ) ims_city
# MAGIC on ym.joinkey = ims_city.joinkey
# MAGIC ;
# MAGIC select * from temp_dim_main;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view temp_link_dim_ims_01
# MAGIC as
# MAGIC select distinct 
# MAGIC        temp_dim_main.ym,
# MAGIC        temp_dim_main.citycode,
# MAGIC        ims.defined_mkt,  
# MAGIC        ims.brandcode, 
# MAGIC        ims.product, 
# MAGIC        ims.flag_aspen, 
# MAGIC        ims.focuscompegrp, 
# MAGIC        ims.molecule, 
# MAGIC        ims.corp, 
# MAGIC        ims.manufactory,
# MAGIC        ims.package,
# MAGIC        ims.pack_size,
# MAGIC        ims.nfci_code,
# MAGIC        ims.nfci_description
# MAGIC from temp_dim_main  
# MAGIC left join (
# MAGIC            select 
# MAGIC                 ym,
# MAGIC                 defined_mkt,  
# MAGIC                 brandcode, 
# MAGIC                 product, 
# MAGIC                 flag_aspen, 
# MAGIC                 focuscompegrp, 
# MAGIC                 molecule, 
# MAGIC                 corp, 
# MAGIC                 manufactory,
# MAGIC                 package,
# MAGIC                 pack_size,
# MAGIC                 nfci_code,
# MAGIC                 nfci_description
# MAGIC            from b_fact_ims_city	 
# MAGIC            union
# MAGIC            select 
# MAGIC                 ym,
# MAGIC                 defined_mkt,  
# MAGIC                 brandcode, 
# MAGIC                 product, 
# MAGIC                 flag_aspen, 
# MAGIC                 focuscompegrp, 
# MAGIC                 molecule, 
# MAGIC                 corp, 
# MAGIC                 manufactory,
# MAGIC                 package,
# MAGIC                 pack_size,
# MAGIC                 nfci_code,
# MAGIC                 nfci_description
# MAGIC            from b_fact_ims_chpa	 
# MAGIC
# MAGIC ) ims
# MAGIC on temp_dim_main.ym=ims.ym;
# MAGIC select * from temp_link_dim_ims_01;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table G_LINK_DIM_IMS(	
# MAGIC 	YM               VARCHAR(50) ,
# MAGIC 	CITY_CODE        VARCHAR(200) ,
# MAGIC 	BRAND_CODE       VARCHAR(50) ,
# MAGIC 	CORP             VARCHAR(500) ,
# MAGIC 	PRODUCT          VARCHAR(500) ,
# MAGIC 	MOLECULE         VARCHAR(500) ,
# MAGIC 	DEFINED_MKT      VARCHAR(50) ,
# MAGIC 	FOCUSCOMPEGRP    VARCHAR(500) ,
# MAGIC 	MANUFACTORY      VARCHAR(500) ,
# MAGIC 	FLAG_ASPEN       VARCHAR(50) ,
# MAGIC 	PACKAGE          VARCHAR(500) ,
# MAGIC 	PACK_SIZE        VARCHAR(500) ,
# MAGIC 	NFCI_CODE        VARCHAR(500) ,
# MAGIC 	NFCI_DESCRIPTION VARCHAR(500) 
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from G_LINK_DIM_IMS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from G_LINK_DIM_IMS;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into G_LINK_DIM_IMS
# MAGIC (    YM              ,
# MAGIC 	CITY_CODE       ,
# MAGIC 	BRAND_CODE      ,
# MAGIC 	CORP            ,
# MAGIC 	PRODUCT         ,
# MAGIC 	MOLECULE        ,
# MAGIC 	DEFINED_MKT     ,
# MAGIC 	FOCUSCOMPEGRP   ,
# MAGIC 	MANUFACTORY     ,
# MAGIC 	FLAG_ASPEN      ,
# MAGIC 	PACKAGE         ,
# MAGIC 	PACK_SIZE       ,
# MAGIC 	NFCI_CODE       ,
# MAGIC 	NFCI_DESCRIPTION)
# MAGIC   select     YM  ,
# MAGIC 	          CITYCODE       ,
# MAGIC 		        BRANDCODE      ,
# MAGIC 		        CORP            ,
# MAGIC 		        PRODUCT         ,
# MAGIC 		        MOLECULE        ,
# MAGIC 		        DEFINED_MKT     ,
# MAGIC 		        FOCUSCOMPEGRP   ,
# MAGIC 		        MANUFACTORY     ,
# MAGIC 		        FLAG_ASPEN      ,
# MAGIC 		        PACKAGE         ,
# MAGIC 		        PACK_SIZE       ,
# MAGIC 		        NFCI_CODE       ,
# MAGIC 		        NFCI_DESCRIPTION
# MAGIC   from temp_link_dim_ims_01

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from G_LINK_DIM_IMS
