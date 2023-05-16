## 该文件夹主要包含
1. batch运行记录
2. 依赖验证
3. job运行状态
4. 邮件提醒功能
5. Rerun功能
6. 增量还是全量，传递参数给glue控制



## Postgres DB Information:
```
HOST: database-1.cw7feqnaopjp.ap-northeast-1.rds.amazonaws.com
PORT: 5432
DB Name: postgreDB
USERNAME: postgres
PASSWORD: password123
```

## Airflow Server:
```
http://ec2-35-78-175-197.ap-northeast-1.compute.amazonaws.com:8080/home
username: airflow
password: airflow
```

## Airflow Server Notes:
### make python available (optional)
```sudo ln -s python3 python```

### make python available (optional)
```pip install requirements.txt```

### create .aws if not exists (optional)
```
mkdir ~/.aws/
```
### create config file if not exists (optional)
```
touch ~/.aws/config
```
### add below profile into config
```
[profile airflow-role]
role_arn=arn:aws:iam::497254257696:role/ExecuteGlueService
credential_source = Ec2InstanceMetadata
```

### verify the instance profile can assume the role, must attach role to EC2 first
```
aws sts get-caller-identity --profile airflow-role
```

### set up airflow variables on webui
```
key: python
value: /home/ubuntu/venv/bin/python	

key:main
value: /home/ubuntu/airflow_workspace/main.py
```


### config PYTHON_PATH (optional)
```
sudo su root
vi ~/.bashrc
export PYTHON_PATH="/home/ubuntu"
source ~/.bashrc
echo $PYTHON_PATH
```
