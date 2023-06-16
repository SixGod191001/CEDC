create or replace view rpt_view_fact_ims_city as
select
    distinct md5(concat(fact_ims_city.YM, fact_ims_city.Defined_MKT, fact_ims_city.CityCode, fact_ims_city.BrandCode, fact_ims_city.Product, fact_ims_city.Flag_Aspen, fact_ims_city.FocusCompeGrp, fact_ims_city.Molecule, fact_ims_city.Corp, fact_ims_city.Manufactory, fact_ims_city.PACKAGE, fact_ims_city.PACK_SIZE, fact_ims_city.NFCI_Code, fact_ims_city.NFCI_Description)) as Key_CITY,
    fact_ims_city.YM as YM,
    fact_ims_city.Defined_MKT as Defined_MKT,
    fact_ims_city.CityCode as IMS_CityCode,
    fact_ims_city.BrandCode as BrandCode,
    fact_ims_city.Product as Product,
    fact_ims_city.Flag_Aspen as Flag_Aspen,
    fact_ims_city.FocusCompeGrp as FocusCompeGrp,
    fact_ims_city.Molecule as Molecule,
    fact_ims_city.Corp as Corp,
    fact_ims_city.Manufactory as Manufactory,
    fact_ims_city.PACKAGE as Package,
    fact_ims_city.PACK_SIZE as Pack_Size,
    fact_ims_city.NFCI_Code as NFCI_Code,
    fact_ims_city.NFCI_Description as NFCI_Description,
    fact_ims_city.IMSCITY_Amount as IMSCITY_Amount,
    fact_ims_city.IMSCITY_QTY as IMSCITY_QTY,
    fact_ims_city.etl_timestamp as ETL_TIMESTAMP
from fact_ims_city;