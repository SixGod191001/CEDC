CREATE VIEW apdb.rpt_view_dim_inst AS
select InsID,cpaname,InsID_Aspen,InsName,InsTier,ProvinceCode,CityCode from apdb.dim_party 
where InsID in (select Distinct  InsID from apdb.fact_cpa);