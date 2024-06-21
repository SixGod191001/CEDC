CREATE PROCEDURE sp_link_dim_ims(IN batch_no INT)
begin

drop temporary table if exists temp_dim_main;
drop temporary table if exists temp_link_dim_ims_01;

create temporary table temp_dim_main
as
select ym, citycode
from (
    select distinct ym, 1 as joinkey from fact_cpa_csv
    union
    select distinct ym ,1 as joinkey from fact_ims_city_csv
) ym
left join (
    select citycode, 1 as joinkey from fact_ims_city_csv
    union
    select 'NoIMSCity' as CityCode, 1 as joinkey 
) ims_city
on ym.joinkey = ims_city.joinkey
;

create temporary table temp_link_dim_ims_01
as
select distinct 
       temp_dim_main.ym,
       temp_dim_main.citycode,
       ims.defined_mkt,  
       ims.brandcode, 
       ims.product, 
       ims.flag_aspen, 
       ims.focuscompegrp, 
       ims.molecule, 
       ims.corp, 
       ims.manufactory,
       ims.package,
       ims.pack_size,
       ims.nfci_code,
       ims.nfci_description	   
from temp_dim_main  
left join (
           select 
                ym,
                defined_mkt,  
                brandcode, 
                product, 
                flag_aspen, 
                focuscompegrp, 
                molecule, 
                corp, 
                manufactory,
                package,
                pack_size,
                nfci_code,
                nfci_description
           from fact_ims_city	 
           union
           select 
                ym,
                defined_mkt,  
                brandcode, 
                product, 
                flag_aspen, 
                focuscompegrp, 
                molecule, 
                corp, 
                manufactory,
                package,
                pack_size,
                nfci_code,
                nfci_description
           from fact_ims_chpa	 

) ims
on temp_dim_main.ym=ims.ym
;
delete from link_dim_ims;
insert into link_dim_ims (
    ym,
	city_code,
	brand_code,
	corp,
	product,
	molecule,
	defined_mkt,
	focuscompegrp,
	manufactory,
	flag_aspen,
	package,
	pack_size,
	nfci_code,
	nfci_description
)	
select ym,
	citycode,
	brandcode,
	corp,
	product,
	molecule,
	defined_mkt,
	focuscompegrp,
	manufactory,
	flag_aspen,
	package,
	pack_size,
	nfci_code,
	nfci_description
from temp_link_dim_ims_01
;
commit;
end