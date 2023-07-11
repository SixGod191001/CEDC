## 该文件夹主要包含
1. batch运行记录
2. 依赖验证
3. job运行状态
4. 邮件提醒功能
5. Rerun功能
6. 增量还是全量，传递参数给glue控制



## Postgres DB Information:
```
HOST: ec2-52-192-178-58.ap-northeast-1.compute.amazonaws.com
PORT: 5432
DB Name: postgres
USERNAME: postgresadm
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
### add below profile into config, remove default config
```
[profile ExecuteGlueService]
role_arn=arn:aws:iam::497254257696:role/ExecuteGlueService
credential_source = Ec2InstanceMetadata
```

### verify the instance profile can assume the role, must attach role to EC2 first
```
aws sts get-caller-identity --profile ExecuteGlueService
```

### set up airflow variables on webui
```
key: python
value: /home/ubuntu/venv/bin/python	

key:main
value: /home/ubuntu/airflow_workspace/main.py

key:base_url
value:http://35.77.94.48:8080
```


### config PYTHONPATH (optional)
```
sudo su root
vi ~/.bashrc
export PYTHONPATH="/home/ubuntu"
source ~/.bashrc
echo $PYTHONPATH
```
### install package
```
sudo su root
pip install /home/ubuntu/airflow_workspace/requirements.txt
```

### use root to start airflow
### update airflow.cfg ,api验证方式，允许通过用户名密码调用api
```
auth_backend = airflow.api.auth.backend.basic_auth 
```

### creating a new account with Admin/User/Op roles, using this user to authorize to trigger next dag
### https://base64.us/ convert the user:password to base64 encoded string like Y2VkYzphaXJmbG93
### 'Authorization': 'Basic Y2VkYzphaXJmbG93', using cedc:airflow to authorization