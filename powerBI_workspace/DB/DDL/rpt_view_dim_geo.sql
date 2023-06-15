CREATE VIEW apdb.rpt_view_dim_geo (CityCode,City,CityTier,ProvinceCode,Province,IMS_CityCode,IMS_City) AS
SELECT DISTINCT geo.CityCode, geo.City, geo.CityTier, geo.ProvinceCode, geo.Province,ims_city.CityCode as IMS_CityCode,geo.IMS_City
FROM apdb.fact_ims_city ims_city
LEFT JOIN apdb.dim_geography geo ON ims_city.CityCode = geo.IMS_CityCode
UNION
SELECT DISTINCT cpa.CityCode, geo.City, geo.CityTier, geo.ProvinceCode, geo.Province,geo.IMS_CityCode,geo.IMS_City
FROM apdb.fact_cpa cpa
LEFT JOIN apdb.dim_geography geo ON cpa.CityCode = geo.CityCode;