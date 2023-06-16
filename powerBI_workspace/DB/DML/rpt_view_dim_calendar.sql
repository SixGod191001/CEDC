create or replace view rpt_view_dim_calendar as
select
    distinct dim_calendar.YM as YearMonth,
    dim_calendar.FiscalYear as FiscalYear,
    dim_calendar.Year as Year,
    dim_calendar.Quarter as Quarter,
    dim_calendar.Month as Month
from dim_calendar;