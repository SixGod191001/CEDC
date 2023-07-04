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