create or replace view rpt_view_dim_geo as
select
    distinct geo.CityCode as CityCode,
    geo.City as City,
    geo.CityTier as CityTier,
    geo.ProvinceCode as ProvinceCode,
    geo.Province as Province,
    ims_city.CityCode as IMS_CityCode,
    geo.IMS_City as IMS_City
from fact_ims_city ims_city
left join dim_geography geo on ims_city.CityCode = geo.IMS_CityCode
union
select
    distinct cpa.CityCode as CityCode,
    geo.City as City,
    geo.CityTier as CityTier,
    geo.ProvinceCode as ProvinceCode,
    geo.Province as Province,
    geo.IMS_CityCode as IMS_CityCode,
    geo.IMS_City as IMS_City
from fact_cpa cpa
left join dim_geography geo on cpa.CityCode = geo.CityCode;