-- drop view apdb.rpt_view_dim_calendar;
CREATE VIEW apdb.rpt_view_dim_calendar AS
-- 当前年当前月的日期记录  Current Year Current Month
select Distinct
     YM,
     FiscalYear, 
     Year, 
     Quarter, 
     Month,
     ym AS YearMonth,
    'TY'  as DateType
FROM apdb.dim_calendar
UNION 
-- 生成上一年当前月的日期记录 Previous Year Current Month
select Distinct
     YM,
     FiscalYear, 
     Year, 
     Quarter, 
     Month,
     CONCAT(YEAR-1,month) AS YearMonth,
    'LY'  as DateType
FROM apdb.dim_calendar
UNION
-- 当前年上一月的日期记录 Current Year Previous Month
select Distinct
     YM,
     FiscalYear, 
     Year, 
     Quarter, 
     Month,
     case when month='01' then CONCAT(YEAR-1,'12') else CONCAT(YEAR,LPAD(month-1, 2, '0')) end AS YearMonth,
    'LM'  as DateType
FROM apdb.dim_calendar;