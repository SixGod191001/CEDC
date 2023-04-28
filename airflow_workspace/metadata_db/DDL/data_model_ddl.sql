CREATE TABLE dag_dependence(
   dependence_id integer PRIMARY KEY NOT NULL,
   dag_id integer NOT NULL,
   dag_name varchar(255) NOT NULL,
   dependence_dag_id integer NOT NULL,
   dependency_dag_name varchar(255) NOT NULL,
   insert_date date,
   last_update_date date
);

 

CREATE TABLE dim_dag(
   dag_id integer PRIMARY KEY NOT NULL,
   dag_name varchar(255) NOT NULL,
   trigger_type varchar(40) NOT NULL,
   check_dependence varchar(10) NOT NULL,
   tag varchar(255) NOT NULL,
   insert_date date NOT NULL,
   last_update_date date NOT NULL,
   dag_version varchar(255) NOT NULL,
);

 

CREATE TABLE dim_email(
   email_id integer PRIMARY KEY NOT NULL,
   topic varchar(255) NOT NULL,
   subscription varchar(255) NOT NULL,
   email_type varchar(255) NOT NULL,
   email_header varchar(255) NOT NULL,
   email_body varchar(255) NOT NULL,
   active varchar(40) NOT NULL,
   insert_date date NOT NULL,
   last_update_date date NOT NULL
);

 

CREATE TABLE dim_task(
   task_id integer PRIMARY KEY NOT NULL,
   task_name varchar(255) NOT NULL,
   dag_name varchar(255) NOT NULL,
   Description varchar(255),
   active varchar(40) NOT NULL,
   job_limit varchar(40) NOT NULL,
   insert_date date NOT NULL,
   last_update_date date NOT NULL,
   task_version varchar(255) NOT NULL,
);

 

CREATE TABLE dim_job(
   job_id integer PRIMARY KEY NOT NULL,
   job_name varchar(255) NOT NULL,
   task_name varchar(255) NOT NULL,
   engine_type varchar(40) NOT NULL,
   data_quality_check varchar(40) NOT NULL,
   job_priority integer NOT NULL,
   number_of_retries integer NOT NULL,
   load_type varchar(10) NOT NULL,
   is_deleted varchar(10) NOT NULL,
   insert_date date NOT NULL,
   last_update_date date NOT NULL,
   job_version varchar(255) NOT NULL
);

 

CREATE TABLE dim_job_params(
   params_id integer PRIMARY KEY NOT NULL,
   job_name varchar(255) NOT NULL,
   param_name varchar(255) NOT NULL,
   param_value varchar(40) NOT NULL,
   is_deleted varchar(10) NOT NULL,
   insert_date date NOT NULL,
   last_update_date date NOT NULL
);

 

CREATE TABLE fact_dag_details(
   id integer PRIMARY KEY NOT NULL,
   dag_id integer NOT NULL,
   dependence_id integer NOT NULL,
   start_date date NOT NULL,
   end_date date NOT null,
   status varchar(40) NOT NULL,
   insert_date date NOT NULL,
   last_update_date date NOT NULL
);

 

CREATE TABLE fact_task_details(
   id integer PRIMARY KEY NOT NULL,
   task_id integer NOT NULL,
   start_date date NOT NULL,
   end_date date NOT null,
   status varchar(40) NOT NULL,
   insert_date date NOT NULL,
   last_update_date date NOT NULL
);

 

CREATE TABLE fact_job_details(
   id integer PRIMARY KEY NOT NULL,
   dag_id integer NOT NULL,
   task_id integer NOT NULL,
   job_id integer NOT NULL,
   run_id varchar(255),
   job_start_date date,
   job_end_date date,
   job_status varchar(40),
   insert_date date NOT NULL,
   last_update_date date NOT NULL
);
