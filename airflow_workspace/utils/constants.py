class Constants:
    SQL_GET_LAST_GLUE_STATE = """select fjd.job_status 
from fact_job_details fjd 
join dim_job dj on fjd.job_id = dj.job_id 
where DJ.job_name ='{job_name}'
ORDER BY fjd.id DESC
LIMIT 1"""
