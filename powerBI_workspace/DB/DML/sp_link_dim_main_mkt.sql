CREATE PROCEDURE sp_link_dim_main_mkt(IN batch_no INT)
begin

drop temporary table if exists temp_ym;
drop temporary table if exists temp_link_dim_main_mkt_01;
drop temporary table if exists temp_link_dim_main_mkt_02;
drop temporary table if exists temp_org_insid;
drop temporary table if exists temp_org_noinsid;

create temporary table temp_ym 
as 
select distinct ym, 1 as joinkey from fact_cpa
union 
select distinct ym ,1 as joinkey from fact_ims_city
;

create temporary table temp_link_dim_main_mkt_01
as
select main_dim.ym,
       main_dim.citycode,
	   main_dim.ims_citycode,
       mkt.defined_mkt,
       mkt.brandcode
from (
       select ym.ym,
              cap_city.citycode,
	          cap_city.ims_citycode
        from temp_ym ym
        left join 
              (select distinct 
        	          cpa.citycode,
                      ims_citycode,
        			  1 as joinkey
              from fact_cpa cpa
              left join dim_geography geo on cpa.citycode=geo.citycode and ims_citycode is not null
               ) cap_city on ym.joinkey=cap_city.joinkey
	   
	  ) main_dim 
left join ( 
           select ym,
                  defined_mkt,
                  brandcode
           from fact_cpa
           union 
           select ym,
                  defined_mkt,
                  brand_code as brandcode
           from link_dim_ims
           ) mkt
 on main_dim.ym=mkt.ym
;

create temporary table temp_org_insid
as
select temp_ym.ym,
       repline.insid,
	   repline.brandcode,
	   repline.citycode,
       repline.provincecode,
       repline.rsm_code, 
       repline.dsm_code, 
       repline.mr_code, 
       repline.dsm, 
       repline.rsm, 
       repline.mr, 
       repline.team, 
       repline.region, 
       repline.rsd_code, 
       repline.rsd
from temp_ym
left join dim_report_line repline on 1=1 
;
create temporary table temp_org_noinsid
as
select distinct 
       temp_ym.ym,
	   repline.brandcode,
	   repline.citycode,
       repline.provincecode,
       repline.rsm_code, 
       repline.dsm_code, 
       repline.mr_code, 
       repline.dsm, 
       repline.rsm, 
       repline.mr, 
       repline.team, 
       repline.region, 
       repline.rsd_code, 
       repline.rsd
from temp_ym
left join dim_report_line repline on 1=1
;

create temporary table temp_link_dim_main_mkt_02
as
select main_link.ym,
       main_link.citycode,
	   main_link.ims_citycode,
       main_link.defined_mkt,
       main_link.brandcode,
	   fact_cpa.insid,
	   dim_party.insid_aspen,
	   geo.provincecode,
       ifnull(org_insid.rsm_code,org_noinsid.rsm_code) as rsm_code, 
       ifnull(org_insid.dsm_code,org_noinsid.dsm_code) as dsm_code,
       ifnull(org_insid.mr_code,org_noinsid.mr_code) as mr_code,
       ifnull(org_insid.dsm,org_noinsid.dsm) as dsm,
       ifnull(org_insid.rsm,org_noinsid.rsm) as rsm,
       ifnull(org_insid.mr,org_noinsid.mr) as mr,
       ifnull(org_insid.team,org_noinsid.team) as team,
       ifnull(org_insid.region,org_noinsid.region) as region,
       ifnull(org_insid.rsd_code,org_noinsid.rsd_code) as rsd_code,
       ifnull(org_insid.rsd,org_noinsid.rsd) as rsd
from temp_link_dim_main_mkt_01 main_link
left join fact_cpa 
       on main_link.ym=fact_cpa.ym
      and main_link.defined_mkt=fact_cpa.defined_mkt
      and main_link.brandcode=fact_cpa.brandcode
      and main_link.citycode=fact_cpa.citycode
left join dim_party on fact_cpa.insid=dim_party.insid
left join dim_geography geo on main_link.citycode=geo.citycode
left join temp_org_insid org_insid
       on fact_cpa.insid=org_insid.insid
	  and main_link.brandcode=org_insid.brandcode
	  and main_link.citycode=org_insid.citycode
      and geo.provincecode=org_insid.provincecode
left join temp_org_noinsid org_noinsid
       on main_link.ym=org_noinsid.ym
	  and main_link.brandcode=org_noinsid.brandcode
	  and main_link.citycode=org_noinsid.citycode
      and geo.provincecode=org_noinsid.provincecode

;

delete from link_dim_main_mkt;
insert into link_dim_main_mkt (
           ym,
           citycode,
           ims_citycode,
           defined_mkt,
           brandcode,
           insid,
           insid_aspen,
           provincecode,
           rsm_code,
           rsm,
           rsd_code,
           rsd,
           region,
           dsm_code,
           dsm,
           mr_code,
           mr,
           team
)
select     ym,
           citycode,
           ims_citycode,
           defined_mkt,
           brandcode,
           insid,
           insid_aspen,
           provincecode,
           rsm_code,
           rsm,
           rsd_code,
           rsd,
           region,
           dsm_code,
           dsm,
           mr_code,
           mr,
           team
from temp_link_dim_main_mkt_02;
commit;

end