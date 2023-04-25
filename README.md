![CEDC](https://gitee.com/SixGod2019/shared-info/raw/master/github_images/images/cedc-logo.png)
## Backgroud
This project is aiming to build a whole cloud based DevOps ETL process. Include below Parts:
1. Cloud Infrastructure
   - Jenkins
   - Airflow
2. Airflow framework
3. Jenkins Devops Pipeline
4. Glue ETL Common Solution
5. Multi-account architecture



## Project Name
<font color=red>**C**</font>loud base <font color=red>**E**</font>TL <font color=red>**D**</font>evOps process of <font color=red>**C**</font>ommunity = <font color=red>**CEDC**</font>

## Project Directory
- [Airflow Framework](https://github.com/SixGod191001/CEDC/tree/main/airflow_framework/workspace)
- [Glue Script Generator and common templates](https://github.com/SixGod191001/CEDC/tree/main/common)
- [Documents](https://github.com/SixGod191001/CEDC/tree/main/documents)
  - [Drawio](https://github.com/SixGod191001/CEDC/tree/main/documents/drawio)
  - [Docs](https://github.com/SixGod191001/CEDC/tree/main/documents/drawio/docs)
- [Jenkins](https://github.com/SixGod191001/CEDC/tree/main/jenkins_framework)
  - [Jenkins Infrastructure](https://github.com/SixGod191001/CEDC/tree/main/jenkins_framework/serverless_jenkins_on_aws_fargate_cloudformation)
  - [Jenkins Pipelines](https://github.com/SixGod191001/CEDC/tree/main/jenkins_framework/pipeline)
- [Sample Code](https://github.com/SixGod191001/CEDC/tree/main/sample_code)
- [Sample Data](https://github.com/SixGod191001/CEDC/tree/main/sample_data)
- [Unit Test](https://github.com/SixGod191001/CEDC/tree/main/tests)
- [Glue Workspace](https://github.com/SixGod191001/CEDC/tree/main/workspace)



## Project Wiki
[Project Wiki](https://github.com/SixGod191001/CEDC/wiki)
  - [Home](https://github.com/SixGod191001/CEDC/wiki)
  - [Q&A](https://github.com/SixGod191001/CEDC/wiki/QA---%E5%B7%B2%E7%9F%A5%E9%97%AE%E9%A2%98-&-%E8%A7%A3%E5%86%B3%E6%96%B9%E6%A1%88)
  

## Project Sprint
[Sprint](https://github.com/users/SixGod191001/projects/2)

## Architecture
### basic logicflow
![](https://gitee.com/SixGod2019/shared-info/raw/master/github_images/images/basic_logicflow.png)



## Cloud Infrastructure
### Account distribution 
- DevOps Account: this is a DevOps account mainly include Jenkins and Airflow
- Data Account: this is a data lake account mainly include S3
- Serverless Account: this is a ETL account mainly include Glue, Lambda etc
- IDP Account: this is a Identity account which can assume A/B/C accounts by **User role** or **Admin Role**

![](https://gitee.com/SixGod2019/shared-info/raw/master/github_images/images/%E5%A4%9A%E8%B4%A6%E6%88%B7%E4%BD%93%E7%B3%BB%E7%BB%93%E6%9E%84.drawio.png)
## jenkins Infrastructure

![](https://d2908q01vomqb2.cloudfront.net/7719a1c782a1ba91c031a682a0a2f8658209adbf/2021/03/24/Jenkins.jpg)


<font color=red>**Note**: in the first draft, we can centralized deploy all services into one account for demo purpose.</font>


## Airflow framework
### Features
- Parameter driven framework
- Check Dependence
- Kickoff
- Monitor
- Job Retry
- Notify
- Metadata backend


## Jenkins DevOps Pipeline
### Features
- Deploy airflow dags and glue job in project
  ![](https://gitee.com/SixGod2019/shared-info/raw/master/github_images/images/jenkins_basic_diagram.png)
- Onboarding/Off Boarding
- Data validation
- Convert SQL to Glue Pyspark

## Glue ETL jobs
### Account prerequisite
### Standard aws serverless account with below items:
- Glue
- Lambda
- S3
- Cloudwatch Events
- Cloudwatch logs
- Secrets manager
- wip ...

### Glue
Glue job naming standard: 
- <project_name>_<table_name or process_name>_prelanding
- <project_name>_<table_name or process_name>_landing
- <project_name>_<table_name or process_name>_landing_merge
- <project_name>_<table_name or process_name>_refinement
- <project_name>_<table_name or process_name>_publish




## IAM Roles Management
1. Serverless Account: Glue Job Execution role -> **DEVOPS_GLUE_CEDC_EXECUTION** (cross account role to ensure Airflow can trigger glue jobs on Account C)
2. DevOps Account: **DEVOPS_GLUE_CEDC_READ**/**DEVOPS_GLUE_CEDC_ADMIN** (Readonly or Admin)
3. IDP Account: CICD Role: **DEVOPS_CICD_CEDC** (which will assume admin access for all accounts for now.)
4. Data Account: **DEVOPS_S3_CEDC_READ**/**DEVOPS_S3_CEDC_ADMIN**
