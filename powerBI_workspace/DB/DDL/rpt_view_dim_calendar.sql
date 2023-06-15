CREATE VIEW apdb.rpt_view_dim_calendar AS
select Distinct
     YM as YearMonth,
     FiscalYear, 
     Year, 
     Quarter, 
     Month
FROM apdb.dim_calendar;