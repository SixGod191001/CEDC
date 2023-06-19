# Databricks notebook source
# MAGIC %md
# MAGIC set up default catalog and database

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog main;
# MAGIC use database powerbi;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view temp_ym 
# MAGIC as 
# MAGIC select distinct ym, 1 as joinkey from b_fact_cpa
# MAGIC union 
# MAGIC select distinct ym ,1 as joinkey from b_fact_ims_city
# MAGIC ;
# MAGIC select * from temp_ym;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view temp_link_dim_main_mkt_01
# MAGIC as
# MAGIC select main_dim.ym,
# MAGIC        main_dim.citycode,
# MAGIC 	main_dim.ims_citycode,
# MAGIC        mkt.defined_mkt,
# MAGIC        mkt.brandcode
# MAGIC from (
# MAGIC        select ym.ym,
# MAGIC               cap_city.citycode,
# MAGIC 	       cap_city.ims_citycode
# MAGIC         from temp_ym ym
# MAGIC         left join 
# MAGIC               (select distinct 
# MAGIC         	        cpa.citycode,
# MAGIC                       geo.ims_citycode,
# MAGIC         		 1 as joinkey
# MAGIC               from b_fact_cpa cpa
# MAGIC               left join b_dim_geography geo on cpa.citycode=geo.citycode and ims_citycode is not null
# MAGIC               ) cap_city on ym.joinkey=cap_city.joinkey
# MAGIC 	   
# MAGIC 	) main_dim 
# MAGIC left join ( 
# MAGIC            select ym,
# MAGIC                   defined_mkt,
# MAGIC                   brandcode
# MAGIC            from b_fact_cpa
# MAGIC            union 
# MAGIC            select ym,
# MAGIC                   defined_mkt,
# MAGIC                   brand_code as brandcode
# MAGIC            from g_link_dim_ims
# MAGIC            ) mkt
# MAGIC  on main_dim.ym=mkt.ym;
# MAGIC  select * from temp_link_dim_main_mkt_01;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view temp_org_insid
# MAGIC as
# MAGIC select temp_ym.ym,
# MAGIC        repline.insid,
# MAGIC 	repline.brandcode,
# MAGIC 	repline.citycode,
# MAGIC        repline.provincecode,
# MAGIC        repline.rsm_code, 
# MAGIC        repline.dsm_code, 
# MAGIC        repline.mr_code, 
# MAGIC        repline.dsm, 
# MAGIC        repline.rsm, 
# MAGIC        repline.mr, 
# MAGIC        repline.team, 
# MAGIC        repline.region, 
# MAGIC        repline.rsd_code, 
# MAGIC        repline.rsd
# MAGIC from temp_ym
# MAGIC left join g_dim_report_line repline on 1=1 
# MAGIC ;
# MAGIC select * from temp_org_insid;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view temp_org_noinsid
# MAGIC as
# MAGIC select distinct 
# MAGIC        temp_ym.ym,
# MAGIC 	repline.brandcode,
# MAGIC 	repline.citycode,
# MAGIC        repline.provincecode,
# MAGIC        repline.rsm_code, 
# MAGIC        repline.dsm_code, 
# MAGIC        repline.mr_code, 
# MAGIC        repline.dsm, 
# MAGIC        repline.rsm, 
# MAGIC        repline.mr, 
# MAGIC        repline.team, 
# MAGIC        repline.region, 
# MAGIC        repline.rsd_code, 
# MAGIC        repline.rsd
# MAGIC from temp_ym
# MAGIC left join g_dim_report_line repline on 1=1
# MAGIC ;
# MAGIC select * from temp_org_noinsid;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view temp_link_dim_main_mkt_02
# MAGIC as
# MAGIC select main_link.ym,
# MAGIC        main_link.citycode,
# MAGIC 	main_link.ims_citycode,
# MAGIC        main_link.defined_mkt,
# MAGIC        main_link.brandcode,
# MAGIC 	fact_cpa.insid,
# MAGIC 	dim_party.insid_aspen,
# MAGIC 	geo.provincecode,
# MAGIC        ifnull(org_insid.rsm_code,org_noinsid.rsm_code) as rsm_code, 
# MAGIC        ifnull(org_insid.dsm_code,org_noinsid.dsm_code) as dsm_code,
# MAGIC        ifnull(org_insid.mr_code,org_noinsid.mr_code) as mr_code,
# MAGIC        ifnull(org_insid.dsm,org_noinsid.dsm) as dsm,
# MAGIC        ifnull(org_insid.rsm,org_noinsid.rsm) as rsm,
# MAGIC        ifnull(org_insid.mr,org_noinsid.mr) as mr,
# MAGIC        ifnull(org_insid.team,org_noinsid.team) as team,
# MAGIC        ifnull(org_insid.region,org_noinsid.region) as region,
# MAGIC        ifnull(org_insid.rsd_code,org_noinsid.rsd_code) as rsd_code,
# MAGIC        ifnull(org_insid.rsd,org_noinsid.rsd) as rsd
# MAGIC from temp_link_dim_main_mkt_01 main_link
# MAGIC left join b_fact_cpa as fact_cpa
# MAGIC        on main_link.ym=fact_cpa.ym
# MAGIC       and main_link.defined_mkt=fact_cpa.defined_mkt
# MAGIC       and main_link.brandcode=fact_cpa.brandcode
# MAGIC       and main_link.citycode=fact_cpa.citycode
# MAGIC left join b_dim_party as dim_party on fact_cpa.insid=dim_party.insid
# MAGIC left join b_dim_geography geo on main_link.citycode=geo.citycode
# MAGIC left join temp_org_insid org_insid
# MAGIC        on fact_cpa.insid=org_insid.insid
# MAGIC       and main_link.brandcode=org_insid.brandcode
# MAGIC       and main_link.citycode=org_insid.citycode
# MAGIC       and geo.provincecode=org_insid.provincecode
# MAGIC left join temp_org_noinsid org_noinsid
# MAGIC        on main_link.ym=org_noinsid.ym
# MAGIC       and main_link.brandcode=org_noinsid.brandcode
# MAGIC       and main_link.citycode=org_noinsid.citycode
# MAGIC       and geo.provincecode=org_noinsid.provincecode
# MAGIC       ;
# MAGIC   select * from temp_link_dim_main_mkt_02;  
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE  or replace table  g_LINK_DIM_MAIN_MKT(	
# MAGIC     YM           VARCHAR(50),
# MAGIC     CITYCODE     VARCHAR(50),
# MAGIC 	IMS_CITYCODE VARCHAR(50),
# MAGIC     DEFINED_MKT  VARCHAR(50),
# MAGIC     BRANDCODE    VARCHAR(50),
# MAGIC 	INSID        VARCHAR(50),
# MAGIC 	INSID_ASPEN  VARCHAR(50),
# MAGIC 	PROVINCECODE VARCHAR(50),
# MAGIC     RSM_CODE     VARCHAR(50), 
# MAGIC     RSM          VARCHAR(50),   
# MAGIC     RSD_CODE     VARCHAR(50),
# MAGIC     RSD          VARCHAR(50),	   
# MAGIC     REGION       VARCHAR(500),	   
# MAGIC     DSM_CODE     VARCHAR(50),
# MAGIC 	DSM          VARCHAR(50),
# MAGIC     MR_CODE      VARCHAR(50),
# MAGIC     MR           VARCHAR(50),	   
# MAGIC     TEAM         VARCHAR(500)
# MAGIC )	
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from g_link_dim_main_mkt;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into g_link_dim_main_mkt (
# MAGIC            ym,
# MAGIC            citycode,
# MAGIC            ims_citycode,
# MAGIC            defined_mkt,
# MAGIC            brandcode,
# MAGIC            insid,
# MAGIC            insid_aspen,
# MAGIC            provincecode,
# MAGIC            rsm_code,
# MAGIC            rsm,
# MAGIC            rsd_code,
# MAGIC            rsd,
# MAGIC            region,
# MAGIC            dsm_code,
# MAGIC            dsm,
# MAGIC            mr_code,
# MAGIC            mr,
# MAGIC            team
# MAGIC )
# MAGIC select     ym,
# MAGIC            citycode,
# MAGIC            ims_citycode,
# MAGIC            defined_mkt,
# MAGIC            brandcode,
# MAGIC            insid,
# MAGIC            insid_aspen,
# MAGIC            provincecode,
# MAGIC            rsm_code,
# MAGIC            rsm,
# MAGIC            rsd_code,
# MAGIC            rsd,
# MAGIC            region,
# MAGIC            dsm_code,
# MAGIC            dsm,
# MAGIC            mr_code,
# MAGIC            mr,
# MAGIC            team
# MAGIC from temp_link_dim_main_mkt_02;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from g_link_dim_main_mkt;

# COMMAND ----------


