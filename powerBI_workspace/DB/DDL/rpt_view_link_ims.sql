CREATE VIEW apdb.rpt_view_link_ims AS
select Distinct
     YM &'|'& Defined_MKT &'|'& BRAND_CODE &'|'& City_Code as Key_IMS,
     YM &'|'& Defined_MKT &'|'& BRAND_CODE &'|'& Product &'|'& Flag_Aspen &'|'& FocusCompeGrp &'|'& Molecule &'|'& Corp &'|'& Manufactory &'|'& Package &'|'& Pack_Size &'|'& NFCI_Code &'|'& NFCI_Description as Key_CHPA,
     YM &'|'& Defined_MKT &'|'& City_Code &'|'& BRAND_CODE &'|'& Product &'|'& Flag_Aspen &'|'& FocusCompeGrp &'|'& Molecule &'|'& Corp &'|'& Manufactory &'|'& Package &'|'& Pack_Size &'|'& NFCI_Code &'|'& NFCI_Description as Key_CITY,
     City_Code as IMS_CityCode,
     YM, 
     Defined_MKT, 
     BRAND_CODE as BrandCode, 
     Product  as IMS_Product, 
     Flag_Aspen      as IMS_Flag_Aspen, 
     FocusCompeGrp   as IMS_FocusCompeGrp, 
     Molecule        as IMS_Molecule, 
     Corp            as IMS_Corp, 
     Manufactory     as IMS_Manufactory,
     Package         as IMS_Package,
     Pack_Size       as IMS_Pack_Size,
     NFCI_Code       as IMS_NFCI_Code,
     NFCI_Description as IMS_NFCI_Description
from apdb.link_dim_ims;