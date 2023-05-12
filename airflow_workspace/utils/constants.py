class Constants:
    SQL_GET_LAST_GLUE_STATE = """select fjd.job_status 
from fact_job_details fjd 
join dim_job dj on fjd.job_id = dj.job_id 
where DJ.job_name ='{job_name}'
ORDER BY fjd.id DESC
LIMIT 1"""

    SQL_GET_JOB_STATE = """SELECT job_status FROM fact_job_details WHERE job_id in
(SELECT job_id FROM dim_job WHERE job_name = '{job_name}')
ORDER BY last_update_date desc LIMIT 1"""

    SAL_GET_EMAIL = """select email_header,email_body
from dim_email
WHERE topic = '{topic}' AND email_type = '{email_type}' AND is_active = 'Y';
    """
