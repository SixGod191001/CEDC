# Databricks notebook source
# MAGIC %md
# MAGIC set up default catalog and database

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog main;
# MAGIC use database powerbi;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view temp_reportline_1
# MAGIC as
# MAGIC select distinct
# MAGIC            t1.ym,	  
# MAGIC            t1.mr_code, 
# MAGIC            t1.mr, 
# MAGIC            t1.Team_Name_cn team, 
# MAGIC            t1.dsm_code, 
# MAGIC            t1.dsm, 
# MAGIC       	   t2.insid_aspen,
# MAGIC       	   t2.brandcode,
# MAGIC 		       -- t3.provincecode,
# MAGIC       	   coalesce(t3.insid,cpa.insid) as insid,
# MAGIC       	   coalesce(t3.provincecode,cpa.provincecode,ims_city.provincecode) as provincecode,
# MAGIC       	   t3.citycode,
# MAGIC 		       case when (upper(t2.brandcode)='B002' or upper(t2.brandcode)='B004') then 'ANA' else 'THR' end as bu,
# MAGIC            case when t1.ym is not null then 1 else 0 end as flag_terr
# MAGIC       from b_dim_territory t2 
# MAGIC       left join b_dim_org t1 on t1.mr_code=t2.mr_code and t1.ym=t2.ym
# MAGIC       left join b_dim_party t3 on t2.insid_aspen=t3.insid_aspen
# MAGIC       full join (select distinct 
# MAGIC                              cpa.brandcode , 
# MAGIC                              cpa.citycode, 
# MAGIC                              cpa.insid,
# MAGIC                         	   geo.provincecode
# MAGIC                         from b_fact_cpa cpa
# MAGIC                         left join (
# MAGIC                                     select provincecode, 
# MAGIC                                            citycode
# MAGIC                                     from b_dim_geography
# MAGIC                                     union 
# MAGIC                                     select provincecode, 
# MAGIC                                           ims_citycode
# MAGIC                                     from b_dim_geography	 
# MAGIC                                    ) geo on cpa.citycode=geo.citycode
# MAGIC                         where cpa.brandcode is not null
# MAGIC                         ) cpa 
# MAGIC       on  t2.brandcode=cpa.brandcode 
# MAGIC       and t3.citycode=cpa.citycode 
# MAGIC 			and t3.provincecode=cpa.provincecode
# MAGIC 			and t3.insid=cpa.insid
# MAGIC 			
# MAGIC       full join (
# MAGIC                        select distinct
# MAGIC                             ims_city.brandcode , 
# MAGIC                             ims_city.citycode, 
# MAGIC                        	    geo.provincecode
# MAGIC                        from b_fact_ims_city ims_city
# MAGIC                        left join (
# MAGIC                                     select provincecode, 
# MAGIC                                            citycode
# MAGIC                                     from b_dim_geography
# MAGIC                                     union 
# MAGIC                                     select provincecode, 
# MAGIC                                           ims_citycode
# MAGIC                                     from b_dim_geography	 
# MAGIC                                     ) geo on ims_city.citycode=geo.citycode
# MAGIC                        where ims_city.brandcode is not null
# MAGIC                        ) ims_city
# MAGIC       on  t2.brandcode=ims_city.brandcode 
# MAGIC       and t3.citycode=ims_city.citycode 
# MAGIC 			and t3.provincecode=ims_city.provincecode
# MAGIC       
# MAGIC       ;
# MAGIC       select * from temp_reportline_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view temp_reportline_2
# MAGIC as
# MAGIC select temp_2.ym,
# MAGIC        temp_2.dsm_code, 
# MAGIC        temp_2.dsm, 
# MAGIC        temp_2.insid_aspen,
# MAGIC        temp_2.brandcode,
# MAGIC        temp_2.citycode,
# MAGIC     	 temp_2.provincecode,
# MAGIC        temp_2.insid,
# MAGIC        temp_1.mr_code,
# MAGIC        temp_1.mr,
# MAGIC        temp_1.team
# MAGIC from (
# MAGIC     select ym,
# MAGIC            dsm_code, 
# MAGIC            dsm, 
# MAGIC            insid_aspen,
# MAGIC            brandcode,
# MAGIC            citycode,
# MAGIC     	    provincecode,
# MAGIC            insid,
# MAGIC            count(distinct flag_terr) as cnt
# MAGIC     from temp_reportline_1
# MAGIC     group by ym,
# MAGIC            dsm_code, 
# MAGIC            dsm, 
# MAGIC            insid_aspen,
# MAGIC            brandcode,
# MAGIC            citycode,
# MAGIC     	    provincecode,
# MAGIC           insid
# MAGIC     having count(distinct flag_terr)=1
# MAGIC    ) as temp_2
# MAGIC   left join temp_reportline_1 temp_1
# MAGIC          on temp_2.ym=temp_1.ym
# MAGIC         and temp_2.provincecode=temp_1.provincecode
# MAGIC         and temp_2.citycode=temp_1.citycode
# MAGIC         and temp_2.dsm_code=temp_1.dsm_code
# MAGIC         and temp_2.dsm=temp_1.dsm
# MAGIC         and temp_2.brandcode=temp_1.brandcode
# MAGIC         and nvl(temp_2.insid,1)=nvl(temp_1.insid,1)
# MAGIC
# MAGIC
# MAGIC ;
# MAGIC select * from temp_reportline_2;
# MAGIC
# MAGIC        

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view temp_reportline_3
# MAGIC as
# MAGIC select rsm_brand.ym,
# MAGIC        t_mpp.provincecode, 
# MAGIC        t_mpp.rsm_trtycode, 
# MAGIC        t_mpp.bu,
# MAGIC        t_mpp.citycode,
# MAGIC 	rsm_brand.brandcode,
# MAGIC 	rsm_brand.rsm, 
# MAGIC        rsm_brand.region,
# MAGIC        rsm_brand.rsd_code, 
# MAGIC        rsm_brand.rsd, 
# MAGIC        rsm_brand.nsd_code, 
# MAGIC        rsm_brand.nsd
# MAGIC from
# MAGIC     ( select distinct 
# MAGIC            mpp.provincecode, 
# MAGIC            mpp.rsm_trtycode, 
# MAGIC 	    mpp.region, 
# MAGIC            mpp.bu,
# MAGIC       	   geo.citycode
# MAGIC       from b_mapping_rm_province mpp
# MAGIC       left join (
# MAGIC                  select provincecode, 
# MAGIC                         citycode
# MAGIC                  from b_dim_geography
# MAGIC                  union 
# MAGIC                  select provincecode, 
# MAGIC                        ims_citycode
# MAGIC                  from b_dim_geography	 
# MAGIC                  ) geo on mpp.provincecode=geo.provincecode
# MAGIC       where city_code is null	  
# MAGIC       union all
# MAGIC       select distinct 
# MAGIC            provincecode, 
# MAGIC            rsm_trtycode, 
# MAGIC            region, 
# MAGIC            bu,
# MAGIC       	   city_code
# MAGIC       from b_mapping_rm_province mpp
# MAGIC       where city_code is not null
# MAGIC     ) t_mpp	  
# MAGIC left join (select distinct
# MAGIC                  t1.ym, 
# MAGIC                  t1.rsm_code,
# MAGIC                  t1.rsm, 
# MAGIC                  t1.region,
# MAGIC                  t1.rsd_code, 
# MAGIC                  t1.rsd, 
# MAGIC                  t1.nsd_code, 
# MAGIC                  t1.nsd,
# MAGIC                  t2.brandcode
# MAGIC            from b_dim_territory t2 
# MAGIC            left join b_dim_org t1 on t1.mr_code=t2.mr_code and t1.ym=t2.ym
# MAGIC 		   ) rsm_brand
# MAGIC on t_mpp.rsm_trtycode=rsm_brand.rsm_code;
# MAGIC select * from temp_reportline_3;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table G_DIM_REPORT_LINE(	
# MAGIC 	INSID        VARCHAR(50),
# MAGIC     BRANDCODE    VARCHAR(50),
# MAGIC     CITYCODE     VARCHAR(50),
# MAGIC 	PROVINCECODE VARCHAR(50),
# MAGIC     MR           VARCHAR(50),
# MAGIC     MR_CODE      VARCHAR(50),	
# MAGIC 	DSM          VARCHAR(50),	
# MAGIC     DSM_CODE     VARCHAR(50),	
# MAGIC     TEAM         VARCHAR(500),
# MAGIC     RSM_CODE     VARCHAR(50), 
# MAGIC     RSM          VARCHAR(50),  
# MAGIC     REGION       VARCHAR(500),
# MAGIC 	RSD_CODE     VARCHAR(50),
# MAGIC     RSD          VARCHAR(50),
# MAGIC     NSD_Code     VARCHAR(50),
# MAGIC     NSD          VARCHAR(50)
# MAGIC )	;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from G_DIM_REPORT_LINE;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into G_DIM_REPORT_LINE (	
# MAGIC         insid,
# MAGIC         brandcode,
# MAGIC         citycode,
# MAGIC         provincecode ,
# MAGIC         mr,
# MAGIC         mr_code,	
# MAGIC         dsm,	
# MAGIC         dsm_code,	
# MAGIC         team,
# MAGIC         rsm_code, 
# MAGIC         rsm,  
# MAGIC         region,
# MAGIC         rsd_code,
# MAGIC         rsd,
# MAGIC         nsd_code,
# MAGIC         nsd
# MAGIC )	
# MAGIC select  
# MAGIC       temp_reportline_2.insid,
# MAGIC       temp_reportline_2.brandcode,
# MAGIC       temp_reportline_2.citycode,	
# MAGIC       temp_reportline_2.provincecode,
# MAGIC       temp_reportline_2.mr, 
# MAGIC       temp_reportline_2.mr_code, 		
# MAGIC       temp_reportline_2.dsm, 
# MAGIC       temp_reportline_2.dsm_code, 
# MAGIC       temp_reportline_2.team, 
# MAGIC       temp_reportline_3.rsm_trtycode as rsm_code,
# MAGIC       temp_reportline_3.rsm, 
# MAGIC       temp_reportline_3.region,		
# MAGIC       temp_reportline_3.rsd_code, 
# MAGIC       temp_reportline_3.rsd, 
# MAGIC       temp_reportline_3.nsd_code, 
# MAGIC       temp_reportline_3.nsd
# MAGIC from temp_reportline_2
# MAGIC left join temp_reportline_3
# MAGIC on temp_reportline_2.provincecode=temp_reportline_3.provincecode
# MAGIC and   temp_reportline_2.citycode=temp_reportline_3.citycode
# MAGIC and   temp_reportline_2.brandcode=temp_reportline_3.brandcode
# MAGIC and   temp_reportline_2.ym=temp_reportline_3.ym
# MAGIC where temp_reportline_2.citycode is not null
# MAGIC and temp_reportline_2.insid is not null
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY  G_DIM_REPORT_LINE;
