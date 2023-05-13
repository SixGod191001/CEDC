CREATE SEQUENCE dim_dag_dependence_id_seq
INCREMENT 1
START 1
MINVALUE 1
MAXVALUE 99999999999
CACHE 1;


drop table dim_dag_dependence;
CREATE TABLE dim_dag_dependence(
   dependence_id             int not null default nextval('dim_dag_dependence_id_seq') primary key,
   dag_id                    integer NOT NULL,
   dag_name                  varchar(500) NOT NULL,
   dependence_dag_id         integer NOT NULL,
   dependency_dag_name       varchar(500) NOT NULL,
   is_active                 varchar(40) NOT NULL,
   insert_date               timestamp,
   last_update_date          timestamp
);
comment on table  dim_dag_dependence                       is 'Dag之间关系依赖表';
comment on column dim_dag_dependence.dependence_id         is '自增ID(逻辑主键)';
comment on column dim_dag_dependence.dag_id                is 'dag的id';
comment on column dim_dag_dependence.dag_name              is 'dag的名字';
comment on column dim_dag_dependence.dependence_dag_id     is '依赖的dag id';
comment on column dim_dag_dependence.dependency_dag_name   is '依赖的dag 名字';
comment on column dim_dag_dependence.is_active             is '该依赖关系是否有效'; --新增字段
comment on column dim_dag_dependence.insert_date           is '插入时间';
comment on column dim_dag_dependence.last_update_date      is '最后更新时间';

CREATE SEQUENCE dim_dag_id_seq
INCREMENT 1
START 1
MINVALUE 1
MAXVALUE 99999999
CACHE 1; 
drop table dim_dag;
CREATE TABLE dim_dag(
   dag_id                   int not null default nextval('dim_dag_id_seq'),
   dag_name                 varchar(500) NOT NULL,
   description              varchar(500),   
   owner                    varchar(255),   
   default_view             varchar(255),   
   trigger_type             varchar(255),
   schedule_interval        varchar(255),    
   next_dagrun              timestamp ,          
   is_check_dependence      varchar(10), 
   concurrency              integer,        
   tag                      varchar(255),
   fileloc                  varchar(1000),
   is_active                varchar(40), 
   insert_date              timestamp,
   last_update_date         timestamp ,
   dag_version              varchar(255),
   PRIMARY KEY(dag_id,dag_name)   
);
comment on table  dim_dag                       is 'Airflow里具体的dag';
comment on column dim_dag.dag_id                is 'dag的id,自增ID';
comment on column dim_dag.dag_name              is 'dag的名字';
comment on column dim_dag.description           is 'dag的描述';
comment on column dim_dag.owner                 is 'dag的所有者';
comment on column dim_dag.default_view          is 'dag的默认展示方式tree view';
comment on column dim_dag.trigger_type          is 'dag触发的方式(on demand/schedule)';
comment on column dim_dag.schedule_interval     is 'dag的schedule时间';
comment on column dim_dag.next_dagrun           is 'dag的下次运行时间';
comment on column dim_dag.is_check_dependence   is 'dag是否需要检查有依赖'; 
comment on column dim_dag.concurrency           is 'dag并发数'; 
comment on column dim_dag.tag                   is 'dag的标签'; 
comment on column dim_dag.fileloc               is '存放dag python脚本的路径'; 
comment on column dim_dag.is_active             is 'dag是否有效'; --新加字段
comment on column dim_dag.insert_date           is 'dag初始化插入时间'; 
comment on column dim_dag.last_update_date      is '最后更新时间'; 
comment on column dim_dag.dag_version           is 'dag版本'; 

CREATE SEQUENCE dim_email_id_seq
INCREMENT 1
START 1
MINVALUE 1
MAXVALUE 99999999
CACHE 1; 
drop table dim_email;
CREATE TABLE dim_email(
   email_id                 int not null default nextval('dim_email_id_seq') primary key,
   topic                    varchar(500) NOT NULL,
   subscription             varchar(500) ,
   email_type               varchar(500) NOT NULL,
   email_header             varchar(500) NOT NULL,
   email_body               varchar(500) NOT NULL,
   is_active                varchar(40), 
   insert_date              timestamp,
   last_update_date         timestamp
);
comment on table  dim_email                       is '邮件维度表';
comment on column dim_email.email_id              is '发送邮件的id,自增ID';
comment on column dim_email.topic                 is '邮件主题';
comment on column dim_email.subscription          is '订阅'; 
comment on column dim_email.email_type            is '邮件的类型'; 
comment on column dim_email.email_header          is '邮件的头'; 
comment on column dim_email.email_body            is '邮件的主体'; 
comment on column dim_email.is_active             is '是否有效'; 
comment on column dim_email.insert_date           is '插入时间'; 
comment on column dim_email.last_update_date      is '最后更新时间'; 

CREATE SEQUENCE dim_task_id_seq
INCREMENT 1
START 1
MINVALUE 1
MAXVALUE 99999999
CACHE 1; 
drop table dim_task;
CREATE TABLE dim_task(
   task_id                  int not null default nextval('dim_task_id_seq') primary key,
   task_name                varchar(500) NOT NULL,
   dag_name                 varchar(500) NOT NULL,
   Description              varchar(500),
   is_active                varchar(40),
   job_limit                varchar(40),
   priority_weight          int,
   max_tries                int,
   insert_date              timestamp,
   last_update_date         timestamp,
   task_version             varchar(255) NOT NULL
);
comment on table  dim_task                       is 'Task表';
comment on column dim_task.task_id               is 'Task的id,自增ID';
comment on column dim_task.task_name             is 'Task的名字';
comment on column dim_task.dag_name              is 'Dag的名字';
comment on column dim_task.Description           is 'Task描述';
comment on column dim_task.is_active             is '是否有效';
comment on column dim_task.job_limit             is 'Job的限制数';
comment on column dim_task.priority_weight       is 'task执行顺序';
comment on column dim_task.max_tries             is 'task最大retry次数';
comment on column dim_task.insert_date           is '插入时间';
comment on column dim_task.last_update_date      is '最后更新时间';
comment on column dim_task.task_version          is 'Task的版本';
 
CREATE SEQUENCE dim_job_id_seq
INCREMENT 1
START 1
MINVALUE 1
MAXVALUE 99999999
CACHE 1; 
CREATE TABLE dim_job(
   job_id                  int not null default nextval('dim_job_id_seq') primary key,
   job_name                varchar(500) NOT NULL,
   task_name               varchar(500) NOT NULL,
   job_type                varchar(255),
   is_data_quality_check   varchar(255),
   job_priority            integer,
   max_retries             integer,
   load_type               varchar(255),
   s3_bucket               varchar(500),
   s3_location             varchar(1000),
   is_active               varchar(40),
   insert_date             timestamp,
   last_update_date        timestamp,
   job_version             varchar(255)
);
comment on table  dim_job                         is 'Job表';
comment on column dim_job.job_id                  is 'Job的id,自增ID';
comment on column dim_job.job_name                is 'Job的名字';
comment on column dim_job.task_name               is 'Task的名字';
comment on column dim_job.job_type                is 'Job的类型(Glue/python/lambda)';
comment on column dim_job.is_data_quality_check   is '是否进行数据质量检查';
comment on column dim_job.job_priority            is 'Job的优先级';
comment on column dim_job.max_retries             is 'Job的最多retry次数';
comment on column dim_job.load_type               is 'Job的加载类型（全量/增量）';
comment on column dim_job.s3_bucket               is 'Job脚本存储的S3桶';
comment on column dim_job.s3_location             is 'Job脚本存储的路径';
comment on column dim_job.is_active               is 'Job是否有效';
comment on column dim_job.insert_date             is '插入时间';
comment on column dim_job.last_update_date        is '最后更新时间';
comment on column dim_job.job_version             is 'Job的版本';

CREATE SEQUENCE dim_job_params_id_seq
INCREMENT 1
START 1
MINVALUE 1
MAXVALUE 99999999
CACHE 1; 
CREATE TABLE dim_job_params(
   params_id               int not null default nextval('dim_job_params_id_seq') primary key,
   job_name                varchar(500) NOT NULL,
   param_name              varchar(1000) NOT NULL,
   param_value             varchar(1000) NOT NULL,
   is_active               varchar(10),
   insert_date             timestamp,
   last_update_date        timestamp
);
comment on table  dim_job_params                            is 'Job的参数表';
comment on column dim_job_params.params_id                  is '参数的id,自增ID';
comment on column dim_job_params.job_name                   is 'Job的名字';
comment on column dim_job_params.param_name                 is '参数的名字';
comment on column dim_job_params.param_value                is '参数的值';
comment on column dim_job_params.is_active                  is '是否有效';
comment on column dim_job_params.insert_date                is '插入时间';
comment on column dim_job_params.last_update_date           is '最后更新时间';
 
CREATE SEQUENCE fact_dag_details_id_seq
INCREMENT 1
START 1
MINVALUE 1
MAXVALUE 99999999
CACHE 1; 
CREATE TABLE fact_dag_details(
   id                       int not null default nextval('fact_dag_details_id_seq') primary key,
   dag_id                   integer NOT NULL,
   dependence_id            integer,
   execution_date           timestamp,
   start_date               timestamp,
   end_date                 timestamp,
   run_id                   varchar(1000),
   status                   varchar(255),
   run_type                 varchar(255),
   last_scheduling_decision timestamp,
   insert_date              timestamp,
   last_update_date         timestamp
);
comment on table  fact_dag_details                              is 'Dag run的事实表';
comment on column fact_dag_details.id                           is 'Dag run表的id,自增ID';
comment on column fact_dag_details.dag_id                       is 'Dag的id';
comment on column fact_dag_details.dependence_id                is '需要依赖的Dag的id';
comment on column fact_dag_details.execution_date               is 'Dag的执行日期';
comment on column fact_dag_details.start_date                   is 'Dag run的开始时间';
comment on column fact_dag_details.end_date                     is 'Dag run的结束时间';
comment on column fact_dag_details.run_id                       is 'Dag每次run的run id';
comment on column fact_dag_details.status                       is 'Dag run的状态（success/running/failed）';
comment on column fact_dag_details.run_type                     is 'Dag run表的类型(scheduled/manual)';
comment on column fact_dag_details.last_scheduling_decision     is 'Dag run上次的调度时间';
comment on column fact_dag_details.insert_date                  is '插入时间';
comment on column fact_dag_details.last_update_date             is '最后更新时间';

 
CREATE SEQUENCE fact_task_details_id_seq
INCREMENT 1
START 1
MINVALUE 1
MAXVALUE 99999999
CACHE 1;
drop table  fact_task_details;
CREATE TABLE fact_task_details(
   id                      int not null default nextval('fact_task_details_id_seq') primary key,
   task_id                 integer,
   dag_id                  integer,
   execution_date          timestamp,
   start_date              timestamp,
   end_date                timestamp,
   duration                decimal(12,6),
   run_id                  varchar(1000),
   status                  varchar(255),
   retry_number              integer,
   priority_weight         integer,
   max_tries               integer,
   insert_date             timestamp,
   last_update_date        timestamp
);
comment on table  fact_task_details                              is 'Task run的事实表';
comment on column fact_task_details.id                           is 'Task run表的id,自增ID';
comment on column fact_task_details.task_id                      is 'task 的id';
comment on column fact_task_details.dag_id                       is 'dag  的id';
comment on column fact_task_details.execution_date               is 'Task的执行时间';
comment on column fact_task_details.start_date                   is 'Task的开始执行时间';
comment on column fact_task_details.end_date                     is 'Task的结束执行时间';
comment on column fact_task_details.duration                     is 'Task的执行时长';
comment on column fact_task_details.run_id                       is 'Task每次run的run id';
comment on column fact_task_details.status                       is 'Task的状态';
comment on column fact_task_details.retry_number                 is 'Task的retry次数';
comment on column fact_task_details.priority_weight              is 'Task的执行顺序';
comment on column fact_task_details.max_tries                    is 'Task的最大retry次数';
comment on column fact_task_details.insert_date                  is '插入时间';
comment on column fact_task_details.last_update_date             is '最后更新时间';

 
CREATE SEQUENCE fact_job_details_id_seq
INCREMENT 1
START 1
MINVALUE 1
MAXVALUE 99999999
CACHE 1;
CREATE TABLE fact_job_details(
   id                      int not null default nextval('fact_job_details_id_seq') primary key,
   dag_id                  integer,
   task_id                 integer,
   job_id                  integer,
   job_type                varchar(255),
   run_id                  varchar(500),
   latest_heartbeat        timestamp,
   job_start_date          timestamp,
   job_end_date            timestamp,
   duration                decimal(12,6),
   job_status              varchar(500),
   insert_date             timestamp,
   last_update_date        timestamp
);
comment on table  fact_job_details                              is 'Job run的事实表';
comment on column fact_job_details.id                           is 'Job run表的id,自增ID';
comment on column fact_job_details.dag_id                       is 'Dag 的id';
comment on column fact_job_details.task_id                      is 'Task的id';
comment on column fact_job_details.job_id                       is 'Job 的id';
comment on column fact_job_details.job_type                     is 'Job的类型(Glue/python/lambda)';
comment on column fact_job_details.run_id                       is 'Job 的run id';
comment on column fact_job_details.latest_heartbeat             is 'Job 最近一次run的时间';
comment on column fact_job_details.job_start_date               is 'Job run的开始时间';
comment on column fact_job_details.job_end_date                 is 'Job run的结束时间';
comment on column fact_job_details.duration                     is 'Job run的时长';
comment on column fact_job_details.job_status                   is 'Job run的状态';
comment on column fact_job_details.insert_date                  is '插入时间';
comment on column fact_job_details.last_update_date             is '最后更新时间';