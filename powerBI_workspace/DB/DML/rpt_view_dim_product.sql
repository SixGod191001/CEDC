create or replace view rpt_view_dim_product as
select distinct 
     dim_brand.BrandCode as BrandCode,
     dim_brand.BrandDesc as BrandDesc
from dim_brand
where dim_brand.BrandCode in (
    select distinct fact_ims_city.BrandCode
    from fact_ims_city
    union
    select distinct fact_ims_chpa.BrandCode
    from fact_ims_chpa
	);