create or replace view rpt_view_dim_inst as
select
    dim_party.InsID as InsID,
    dim_party.cpaname as cpaname,
    dim_party.InsID_Aspen as InsID_Aspen,
    dim_party.InsName as InsName,
    dim_party.InsTier as InsTier,
    dim_party.ProvinceCode as ProvinceCode,
    dim_party.CityCode as CityCode
from dim_party
where dim_party.InsID in (
    select distinct fact_cpa.InsID
    from fact_cpa);