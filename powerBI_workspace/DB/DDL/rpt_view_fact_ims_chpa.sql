CREATE VIEW apdb.rpt_view_fact_ims_chpa AS
select Distinct
     YM &'|'& Defined_MKT &'|'& BrandCode &'|'& Product &'|'& Flag_Aspen &'|'& FocusCompeGrp &'|'& Molecule &'|'& Corp &'|'& Manufactory &'|'& Package &'|'& PACK_SIZE &'|'& NFCI_Code &'|'& NFCI_Description as Key_CHPA,
     YM,
     Defined_MKT,  
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
     IMSCHPA_Amount, 
     IMSCHPA_QTY
from apdb.fact_ims_chpa;