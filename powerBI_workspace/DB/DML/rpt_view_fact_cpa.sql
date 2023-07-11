create or replace view rpt_view_fact_cpa as
select
    distinct md5(concat(fact_cpa.YM, fact_cpa.Defined_MKT, fact_cpa.BrandCode, fact_cpa.CityCode, fact_cpa.InsID)) as Key_CPA,
    fact_cpa.YM as YM,
    fact_cpa.Defined_MKT as CPA_Defined_MKT,
    fact_cpa.BrandCode as CPA_BrandCode,
    fact_cpa.CityCode as CPA_CityCode,
    fact_cpa.InsID as InsID,
    fact_cpa.Molecule as CPA_Molecule,
    fact_cpa.Product as CPA_Product,
    fact_cpa.FocusCompeGrp as CPA_FocusCompeGrp,
    fact_cpa.Manufactory as CPA_Manufactory,
    fact_cpa.Flag_Aspen as CPA_Flag_Aspen,
    fact_cpa.Flag_TargetHP as CPA_Flag_TargetHP,
    fact_cpa.DrugForm as CPA_DrugForm,
    fact_cpa.Route as CPA_Route,
    fact_cpa.Amount as CPA_Amount,
    fact_cpa.QTY as CPA_QTY,
    fact_cpa.etl_timestamp as ETL_TIMESTAMP,
    '1' as Flag_CPA
from fact_cpa;