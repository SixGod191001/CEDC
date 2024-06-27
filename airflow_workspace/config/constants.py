class Constants:
    """
    A collection of constant values and SQL query templates used in the project.

    Attributes:
    - AWS_REGION_NAME (str): The AWS region name.
    - GLUE_* (str): Various status constants for AWS Glue jobs.
    - FORCE_SUCCESS (str): Constant to indicate forced success status.
    - SQL_* (str): SQL query templates for various database operations.
    - AWS_* (str): AWS related service variables

    """

    AWS_REGION_NAME = 'ap-northeast-1'

    # aws secret manager name to store postgres database connection information
    AWS_SECRET_MANAGER_NAME = "cedc/dags/postgres"
    AWS_GLUE_ROLE = 'ExecuteGlueService'

    GLUE_STARTING = 'STARTING'
    GLUE_RUNNING = 'RUNNING'
    GLUE_STOPPING = 'STOPPING'
    GLUE_STOPPED = 'STOPPED'
    GLUE_SUCCEEDED = 'SUCCEEDED'
    GLUE_FAILED = 'FAILED'
    GLUE_TIMEOUT = 'TIMEOUT'
    GLUE_ERROR = 'ERROR'
    GLUE_WAITING = 'WAITING'
    FORCE_SUCCESS = 'FORCE_SUCCESS'

    SQL_GET_JOB_TEMPLATE_NAME = """
        SELECT DISTINCT job_template_name
        FROM dim_job
        WHERE task_name = '{}'
    """

    SQL_GET_FAILED_TASKS_NAME = """
        SELECT task_name
        FROM (
            SELECT 
                task_name, 
                dag_name,
                status,
                ROW_NUMBER() OVER (PARTITION BY task_name ORDER BY start_date DESC) AS row_num
            FROM fact_task_details
        ) t
        WHERE t.row_num = 1 
          AND dag_name = '{}' 
          AND status NOT IN ('RUNNING', 'SUCCESS')
    """

    SQL_GET_TASKS_NAME = """
        SELECT DISTINCT task_name
        FROM dim_task
        WHERE dag_name = '{}'
    """

    SQL_GET_DAG_NAME = """
        SELECT DISTINCT dag_name 
        FROM dim_task 
        WHERE task_name = '{}'
    """

    SQL_GET_JOB_RUNID = """
        SELECT run_id 
        FROM fact_job_details 
        WHERE job_name = '{}' 
        ORDER BY job_start_date DESC 
        LIMIT 1
    """

    SQL_GET_JOB_NAME = """
        SELECT DISTINCT job_name 
        FROM fact_job_details 
        WHERE task_name = '{}'
    """

    SQL_GET_JOB_DATE = """
        SELECT job_start_date 
        FROM fact_job_details fjd 
        WHERE job_name = '{job_name}'
        ORDER BY job_start_date DESC
        LIMIT 1
    """

    SQL_GET_LAST_GLUE_STATE = """
        SELECT fjd.job_status 
        FROM fact_job_details fjd 
        JOIN dim_job dj ON fjd.job_name = dj.job_name 
        WHERE dj.job_name = '{job_name}'
        ORDER BY fjd.id DESC
        LIMIT 1
    """

    SQL_GET_JOB_PARAM = """
        SELECT param_value 
        FROM dim_job_params djp 
        WHERE job_name = '{job_name}'
          AND param_name = '{param_name}'
    """

    SQL_GET_JOB_LIST = """
        SELECT fjd.job_name, fjd.run_id, fjd.last_update_date
        FROM fact_job_details fjd
        JOIN dim_dag dd ON fjd.dag_name = dd.dag_name
        JOIN (
            SELECT f.job_name, MAX(last_update_date) AS lst_upd_dt
            FROM fact_job_details f
            WHERE job_name = f.job_name
            GROUP BY f.job_name
        ) m ON m.job_name = fjd.job_name 
        AND m.lst_upd_dt = fjd.last_update_date
        WHERE dd.dag_name = '{dag_name}'
    """

    SQL_GET_DAG_STATE = """
        SELECT status 
        FROM fact_dag_details 
        WHERE dag_name = '{dag_name}'
        ORDER BY last_update_date DESC 
        LIMIT 1
    """

    SQL_GET_EMAIL = """
        SELECT email_header, email_body
        FROM dim_email
        WHERE topic = '{topic}' 
          AND email_type = '{email_type}' 
          AND is_active = 'Y'
    """

    SQL_GET_JOB_DETAILS = """
        SELECT 
            j.job_name,
            j.job_type,
            j.job_priority,
            j.max_retries,
            j.load_type,
            j.job_template_name,
            j.s3_location AS "--scriptLocation",
            p.param_name || ' param_value ' || p.param_value AS param_value
        FROM public.dim_job j 
        INNER JOIN public.dim_job_params p 
            ON j.job_name = p.job_name 
        WHERE j.batch_name = '{batch_name}' 
        ORDER BY j.job_priority, j.job_name
    """

    SQL_GET_RUNNING_TASK_NAME = """
        SELECT task_name 
        FROM public.fact_task_details
        WHERE task_name = '{task_name}' 
          AND LOWER(status) = 'running'
    """

    SQL_INSERT_TASK_DETAILS = """
        INSERT INTO fact_task_details (
            task_name, dag_name, execution_date, start_date, end_date, duration, 
            run_id, status, retry_number, priority_weight, max_tries, insert_date, last_update_date
        ) 
        SELECT 
            '{task_name}' AS task_name,
            task.dag_name,
            current_timestamp AS execution_date,
            current_timestamp AS start_date,
            NULL AS end_date,
            NULL AS duration,
            NULL AS run_id,
            '{task_status}' AS status,
            3 AS retry_number,
            task.priority_weight,
            task.max_tries,
            current_timestamp AS insert_date,
            current_timestamp AS last_update_date
        FROM dim_task task
        INNER JOIN dim_dag dag ON task.dag_name = dag.dag_name 
        WHERE task.task_name = '{task_name}'
    """

    SQL_FORCE_SUCCESS_TASK = """
        UPDATE fact_task_details 
        SET 
            end_date = current_timestamp,
            status = '{task_status}',
            last_update_date = current_timestamp
        WHERE task_name = '{task_name}' 
          AND LOWER(status) = 'running'
    """

    SQL_GET_LATEST_JOB_RUN_STATUS = """
        SELECT run_id, job_status 
        FROM fact_job_details
        WHERE job_name = '{job_name}'
        ORDER BY last_update_date DESC 
        LIMIT 1
    """

    SQL_INSERT_JOB_DETAILS = """ INSERT INTO FACT_JOB_DETAILS 
                         (DAG_NAME,TASK_NAME,JOB_NAME,RUN_ID,JOB_START_DATE,JOB_END_DATE,JOB_STATUS,INSERT_DATE,LAST_UPDATE_DATE)
                         SELECT DAG.DAG_NAME 
                               ,TASK.TASK_NAME 
                               ,JOB.JOB_NAME 
                               ,'{run_id}' AS RUN_ID 
                               ,CURRENT_TIMESTAMP AS JOB_START_DATE 
                               ,NULL AS JOB_END_DATE 
                               ,'{status}' AS JOB_STATUS 
                               ,CURRENT_TIMESTAMP AS INSERT_DATE 
                              ,CURRENT_TIMESTAMP AS LAST_UPDATE_DATE 
                         FROM DIM_JOB JOB 
                         INNER JOIN DIM_TASK TASK ON JOB.TASK_NAME=TASK.TASK_NAME 
                         INNER JOIN DIM_DAG DAG ON TASK.DAG_NAME=DAG.DAG_NAME 
                         WHERE JOB.JOB_NAME='{job_name}' 
                         """

    SQL_UPDATE_JOB_STATUS = """UPDATE FACT_JOB_DETAILS SET JOB_END_DATE = CURRENT_TIMESTAMP, JOB_STATUS='{status}',
        LAST_UPDATE_DATE=CURRENT_TIMESTAMP WHERE RUN_ID ='{run_id}' AND JOB_NAME = '{job_name}' 
        """
