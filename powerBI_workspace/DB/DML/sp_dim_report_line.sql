CREATE PROCEDURE sp_dim_report_line(IN batch_no INT)
begin

drop temporary table if exists temp_reportline_1;
drop temporary table if exists temp_reportline_2;
drop temporary table if exists temp_reportline_3;
drop temporary table if exists temp_reportline_4;

create temporary table temp_reportline_1
as
select distinct
           t1.ym,	  
           t1.mr_code, 
           t1.mr, 
           t1.Team_Name_cn team, 
           t1.dsm_code, 
           t1.dsm, 
      	   t2.insid_aspen,
      	   t2.brandcode,
		   t3.insid,
		   t3.provincecode,
      	   -- coalesce(t3.insid,cpa.insid) as insid,
      	   -- coalesce(t3.provincecode,cpa.provincecode,ims_city.provincecode) as provincecode,
      	   t3.citycode,
		   case when (upper(t2.brandcode)='B002' or upper(t2.brandcode)='B004') then 'ANA' else 'THR' end as bu,
		   case when t1.ym is not null then 1 else 0 end as flag_terr
      from dim_territory t2 
      left join dim_org t1 on t1.mr_code=t2.mr_code and t1.ym=t2.ym
      left join dim_party t3 on t2.insid_aspen=t3.insid_aspen
/*      left join (select distinct 
                             cpa.brandcode , 
                             cpa.citycode, 
                             cpa.insid,
                        	   geo.provincecode
                        from fact_cpa cpa
                        left join (
                                    select provincecode, 
                                           citycode
                                    from dim_geography
                                    union 
                                    select provincecode, 
                                          ims_citycode
                                    from dim_geography	 
                                   ) geo on cpa.citycode=geo.citycode
                        where cpa.brandcode is not null
                        ) cpa 
      on  t2.brandcode=cpa.brandcode 
      and t3.citycode=cpa.citycode 
			and t3.provincecode=cpa.provincecode
			and t3.insid=cpa.insid
			
      left join (
                       select distinct
                            ims_city.brandcode , 
                            ims_city.citycode, 
                       	    geo.provincecode
                       from fact_ims_city ims_city
                       left join (
                                    select provincecode, 
                                           citycode
                                    from dim_geography
                                    union 
                                    select provincecode, 
                                          ims_citycode
                                    from dim_geography	 
                                    ) geo on ims_city.citycode=geo.citycode
                       where ims_city.brandcode is not null
                       ) ims_city
      on  t2.brandcode=ims_city.brandcode 
      and t3.citycode=ims_city.citycode 
			and t3.provincecode=ims_city.provincecode
*/			
;

create temporary table temp_reportline_2
as
select ym,
       dsm_code, 
       dsm, 
       insid_aspen,
       brandcode,
       citycode,
	    provincecode,
       insid
from (	   
       select ym,
              dsm_code, 
              dsm, 
              insid_aspen,
              brandcode,
              citycode,
       	      provincecode,
              insid,
              count(distinct flag_terr) as cnt
       from temp_reportline_1
       group by ym,
              dsm_code, 
              dsm, 
              insid_aspen,
              brandcode,
              citycode,
       	     provincecode,
             insid
) t
where cnt=1			 
;	  
		  
		  
create temporary table temp_reportline_3
as
select temp_2.ym,
       temp_2.dsm_code, 
       temp_2.dsm, 
       temp_2.insid_aspen,
       temp_2.brandcode,
       temp_2.citycode,
       temp_2.provincecode,
       temp_2.insid,
       temp_1.mr_code,
       temp_1.mr,
       temp_1.team
from temp_reportline_2 temp_2
left join temp_reportline_1 temp_1
         on temp_2.ym=temp_1.ym
        and temp_2.provincecode=temp_1.provincecode
        and temp_2.citycode=temp_1.citycode
        and temp_2.dsm_code=temp_1.dsm_code
        and temp_2.dsm=temp_1.dsm
        and temp_2.brandcode=temp_1.brandcode
        and coalesce(temp_2.insid,1)=coalesce(temp_1.insid,1)
	

;

create temporary table temp_reportline_4
as
select rsm_brand.ym,
       t_mpp.provincecode, 
       t_mpp.rsm_trtycode, 
       t_mpp.bu,
       t_mpp.citycode,
	   rsm_brand.brandcode,
	   rsm_brand.rsm, 
       rsm_brand.region,
       rsm_brand.rsd_code, 
       rsm_brand.rsd, 
       rsm_brand.nsd_code, 
       rsm_brand.nsd
from
    ( select distinct 
           mpp.provincecode, 
           mpp.rsm_trtycode, 
		   mpp.region, 
           mpp.bu,
      	   geo.citycode
      from mapping_rm_province mpp
      left join (
                 select provincecode, 
                        citycode
                 from dim_geography
                 union 
                 select provincecode, 
                       ims_citycode
                 from dim_geography	 
                 ) geo on mpp.provincecode=geo.provincecode
      where city_code is null	  
      union all
      select distinct 
           provincecode, 
           rsm_trtycode, 
           region, 
           bu,
      	   city_code
      from mapping_rm_province mpp
      where city_code is not null
    ) t_mpp	  
left join (select distinct
                 t1.ym, 
                 t1.rsm_code,
                 t1.rsm, 
                 t1.region,
                 t1.rsd_code, 
                 t1.rsd, 
                 t1.nsd_code, 
                 t1.nsd,
                 t2.brandcode
           from dim_territory t2 
           left join dim_org t1 on t1.mr_code=t2.mr_code and t1.ym=t2.ym
		   ) rsm_brand
on t_mpp.rsm_trtycode=rsm_brand.rsm_code;

delete from dim_report_line;
insert into dim_report_line (	
        insid,
        brandcode,
        citycode,
        provincecode ,
        mr,
        mr_code,	
        dsm,	
        dsm_code,	
        team,
        rsm_code, 
        rsm,  
        region,
        rsd_code,
        rsd,
        nsd_code,
        nsd
)	
select  
        temp_reportline_3.insid,
	    temp_reportline_3.brandcode,
	    temp_reportline_3.citycode,	
	    temp_reportline_3.provincecode,
        temp_reportline_3.mr, 
        temp_reportline_3.mr_code, 		
        temp_reportline_3.dsm, 
        temp_reportline_3.dsm_code, 
        temp_reportline_3.team, 
	    temp_reportline_4.rsm_trtycode as rsm_code,
	    temp_reportline_4.rsm, 
	    temp_reportline_4.region,		
	    temp_reportline_4.rsd_code, 
	    temp_reportline_4.rsd, 
	    temp_reportline_4.nsd_code, 
	    temp_reportline_4.nsd
from temp_reportline_3
left join temp_reportline_4
on temp_reportline_3.provincecode=temp_reportline_4.provincecode
and   temp_reportline_3.citycode=temp_reportline_4.citycode
and   temp_reportline_3.brandcode=temp_reportline_4.brandcode
and   temp_reportline_3.ym=temp_reportline_4.ym
where temp_reportline_3.citycode is not null
and temp_reportline_3.insid is not null
;
commit;

end