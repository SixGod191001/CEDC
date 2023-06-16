create or replace view rpt_view_fact_ims_chpa as
select
    distinct md5(concat(fact_ims_chpa.YM, fact_ims_chpa.Defined_MKT, fact_ims_chpa.BrandCode, fact_ims_chpa.Product, fact_ims_chpa.Flag_Aspen, fact_ims_chpa.FocusCompeGrp, fact_ims_chpa.Molecule, fact_ims_chpa.Corp, fact_ims_chpa.Manufactory, fact_ims_chpa.PACKAGE, fact_ims_chpa.PACK_SIZE, fact_ims_chpa.NFCI_Code, fact_ims_chpa.NFCI_Description)) as Key_CHPA,
    fact_ims_chpa.YM as YM,
    fact_ims_chpa.Defined_MKT as Defined_MKT,
    fact_ims_chpa.BrandCode as BrandCode,
    fact_ims_chpa.Product as Product,
    fact_ims_chpa.Flag_Aspen as Flag_Aspen,
    fact_ims_chpa.FocusCompeGrp as FocusCompeGrp,
    fact_ims_chpa.Molecule as Molecule,
    fact_ims_chpa.Corp as Corp,
    fact_ims_chpa.Manufactory as Manufactory,
    fact_ims_chpa.PACKAGE as Package,
    fact_ims_chpa.PACK_SIZE as Pack_Size,
    fact_ims_chpa.NFCI_Code as NFCI_Code,
    fact_ims_chpa.NFCI_Description as NFCI_Description,
    fact_ims_chpa.IMSCHPA_Amount as IMSCHPA_Amount,
    fact_ims_chpa.IMSCHPA_QTY as IMSCHPA_QTY,
    fact_ims_chpa.etl_timestamp as ETL_TIMESTAMP
from fact_ims_chpa;