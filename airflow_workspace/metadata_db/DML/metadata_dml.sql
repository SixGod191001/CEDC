select * from dag_dependence;
select * from dim_dag;
select * from dim_email;
select * from dim_task;
select * from dim_job;
select * from dim_job_params;
select * from fact_dag_details;
select * from fact_task_details;
select * from fact_job_details;


INSERT INTO dim_dag (dag_name, description, owner, default_view, trigger_type, schedule_interval, next_dagrun, is_check_dependence, concurrency, tag, fileloc, is_active, insert_date, last_update_date)
VALUES
('jghagnoa', 'DAG 1 description', 'Owner 1', 'tree view', 'schedule', '0 0 * * *', '2023-05-12 00:00:00', 'N', 1, 'tag1', 'file1.py', 'Y', '2023-05-11 12:30:00', '2023-05-11 12:30:00'),
('jgrvmreakonvgr', 'DAG 1 description', 'Owner 1', 'tree view', 'schedule', '0 0 * * *', '2023-05-12 00:00:00', 'N', 1, 'tag1', 'file1.py', 'Y', '2023-05-11 12:30:00', '2023-05-11 12:30:00');


INSERT INTO dim_dag_dependence (dependence_id, dag_id, dag_name, dependence_dag_id, dependency_dag_name, is_active, insert_date, last_update_date)
VALUES
(1, 1, 'dag1', 2, 'dag2', 'Y', '2023-05-11 10:00:00', '2023-05-11 12:30:00');


INSERT INTO dim_email (email_id, topic, subscription, email_type, email_header, email_body, is_active, insert_date, last_update_date)
VALUES
(1, 'Email 1 topic', 'Email 1 subscription', 'Type 1', 'Header 1', 'Body 1', 'Y', '2023-05-11 10:00:00', '2023-05-11 12:30:00');


INSERT INTO dim_job (job_id, job_name, task_name, job_type, is_data_quality_check, job_priority, max_retries, load_type, s3_bucket, s3_location, is_active, insert_date, last_update_date, job_version)
VALUES
(1, 'Job 1', 'Task 1', 'Type 1', 'Yes', 1, 3, 'Type 2', 'Bucket 1', 'Location 1', 'Y', '2023-05-11 10:00:00', '2023-05-11 12:30:00', 'v1.0');


INSERT INTO dim_job_params (params_id, job_name, param_name, param_value, is_active, insert_date, last_update_date)
VALUES
(1, 'Job 1', 'Param 1', 'Value 1', 'Y', '2023-05-11 10:00:00', '2023-05-11 12:30:00');


INSERT INTO dim_task (task_id, task_name, dag_name, description, is_active, job_limit, priority_weight, max_tries, insert_date, last_update_date, task_version)
VALUES
(1, 'Task 1', 'dag1', 'Task 1 description', 'Y', 'Limit 1', 1, 3, '2023-05-11 10:00:00', '2023-05-11 12:30:00', 'v1.0');

