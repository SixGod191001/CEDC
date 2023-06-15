CREATE VIEW apdb.rpt_view_fact_cpa AS
select Distinct YM &'|'& Defined_MKT &'|'& BrandCode &'|'& CityCode &'|'& InsID  as Key_CPA,
     YM , 
     Defined_MKT as CPA_Defined_MKT, 
     BrandCode as CPA_BrandCode, 
     CityCode as CPA_CityCode,
     InsID,    
     Molecule as CPA_Molecule, 
     Product as CPA_Product, 
     FocusCompeGrp as CPA_FocusCompeGrp, 
     Manufactory as CPA_Manufactory, 
     Flag_Aspen as CPA_Flag_Aspen, 
     Flag_TargetHP as CPA_Flag_TargetHP,
     DrugForm as CPA_DrugForm,
     Route as CPA_Route,
     Amount as CPA_Amount, 
     QTY as CPA_QTY,
     '1' as Flag_CPA
from apdb.fact_cpa; 
