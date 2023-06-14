# Databricks notebook source
# MAGIC %md
# MAGIC 将IMS CHPA和 IMS CITY的公共维度数据抽取并处理成统一的维度数据

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists g_temp_dm_dim_ims;
# MAGIC create table if not exists g_temp_dm_dim_ims as 
# MAGIC with main_dim as (
# MAGIC select ym,citycode
# MAGIC from (select distinct ym, 1 as joinkey from fact_cpa
# MAGIC       union
# MAGIC       select distinct ym ,1 as joinkey from fact_ims_city
# MAGIC 	  ) ym
# MAGIC 	  left join 
# MAGIC       (select citycode ,1 as joinkey   from fact_ims_city
# MAGIC        union
# MAGIC       select 'NoIMSCity' as CityCode 
# MAGIC 	  ) ims_city
# MAGIC 	  on ym.joinkey=ims_city.joinkey
# MAGIC )
# MAGIC select distinct 
# MAGIC        main_dim.ym,
# MAGIC        main_dim.citycode,
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
# MAGIC from main_dim 
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
# MAGIC            from fact_ims_city	 
# MAGIC            union all
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
# MAGIC            from fact_ims_chpa	 
# MAGIC
# MAGIC ) ims
# MAGIC on main_dim.ym=ims.ym
