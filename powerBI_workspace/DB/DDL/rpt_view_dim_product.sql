CREATE VIEW apdb.rpt_view_dim_product AS
select Distinct BrandCode,BrandDesc from apdb.dim_brand 
where BrandCode in (select Distinct BrandCode from apdb.fact_ims_city union select Distinct BrandCode from apdb.fact_ims_chpa);