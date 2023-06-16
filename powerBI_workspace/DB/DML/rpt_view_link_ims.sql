create or replace view rpt_view_link_ims as
select
    md5(concat(link_dim_ims.YM, link_dim_ims.DEFINED_MKT, link_dim_ims.BRAND_CODE, link_dim_ims.CITY_CODE)) as Key_IMS,
    md5(concat(link_dim_ims.YM, link_dim_ims.DEFINED_MKT, link_dim_ims.BRAND_CODE, link_dim_ims.PRODUCT, link_dim_ims.FLAG_ASPEN, link_dim_ims.FOCUSCOMPEGRP, link_dim_ims.MOLECULE, link_dim_ims.CORP, link_dim_ims.MANUFACTORY, link_dim_ims.PACKAGE, link_dim_ims.PACK_SIZE, link_dim_ims.NFCI_CODE, link_dim_ims.NFCI_DESCRIPTION)) as Key_CHPA,
    md5(concat(link_dim_ims.YM, link_dim_ims.DEFINED_MKT, link_dim_ims.CITY_CODE, link_dim_ims.BRAND_CODE, link_dim_ims.PRODUCT, link_dim_ims.FLAG_ASPEN, link_dim_ims.FOCUSCOMPEGRP, link_dim_ims.MOLECULE, link_dim_ims.CORP, link_dim_ims.MANUFACTORY, link_dim_ims.PACKAGE, link_dim_ims.PACK_SIZE, link_dim_ims.NFCI_CODE, link_dim_ims.NFCI_DESCRIPTION)) as Key_CITY,
    link_dim_ims.CITY_CODE as IMS_CityCode,
    link_dim_ims.YM as YM,
    link_dim_ims.DEFINED_MKT as Defined_MKT,
    link_dim_ims.BRAND_CODE as BrandCode,
    link_dim_ims.PRODUCT as IMS_Product,
    link_dim_ims.FLAG_ASPEN as IMS_Flag_Aspen,
    link_dim_ims.FOCUSCOMPEGRP as IMS_FocusCompeGrp,
    link_dim_ims.MOLECULE as IMS_Molecule,
    link_dim_ims.CORP as IMS_Corp,
    link_dim_ims.MANUFACTORY as IMS_Manufactory,
    link_dim_ims.PACKAGE as IMS_Package,
    link_dim_ims.PACK_SIZE as IMS_Pack_Size,
    link_dim_ims.NFCI_CODE as IMS_NFCI_Code,
    link_dim_ims.NFCI_DESCRIPTION as IMS_NFCI_Description
from link_dim_ims;