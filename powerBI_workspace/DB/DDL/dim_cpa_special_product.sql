create VIEW dim_cpa_special_product
AS 
select  *from stg_cpa where DrugName = '利伐沙班';
