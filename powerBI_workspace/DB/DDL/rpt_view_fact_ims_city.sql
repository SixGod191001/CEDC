CREATE VIEW apdb.rpt_view_fact_ims_city AS
select Distinct
     YM &'|'& Defined_MKT &'|'& citycode &'|'& BrandCode &'|'& Product &'|'& Flag_Aspen &'|'& FocusCompeGrp &'|'& Molecule &'|'& Corp &'|'& Manufactory &'|'& Package &'|'& PACK_SIZE &'|'& NFCI_Code &'|'& NFCI_Description as Key_CITY,
     YM, 
     Defined_MKT,
     citycode as IMS_CityCode,
     BrandCode, 
     Product, 
     Flag_Aspen, 
     FocusCompeGrp, 
     Molecule, 
     Corp, 
     Manufactory, 
     Package, 
     PACK_SIZE as Pack_Size,
     NFCI_Code,
     NFCI_Description,
     IMSCITY_Amount, 
     IMSCITY_QTY
from apdb.fact_ims_city;