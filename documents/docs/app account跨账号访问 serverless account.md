## Cross Account Role 
### Objective: Service with role airflow-role in Jacky Account can access the service with role ExecuteGlueService in Carrie Account
### How to use it
- awscli please use as below 
  ```aws s3 ls --profile airflow-role'``` more details see below scenario
- boto3 please use as below
  ```
  from boto3 import Session
  
  # Insert CLI profile name _and_ region
  boto_sess = Session(profile_name='airflow-role', region_name='ap-northeast-1')
  client = boto_sess.client("s3")
  s3_buckets_json=client.list_buckets()
  print(s3_buckets_json)
  ```
***
```
Jacky Account: 875120157787
Service： EC2 Instance
Role: airflow-role (EC2 Admin, S3 Admin)
```
***
```
Carrie Account: 497254257696
Service：Glue, S3(Glue Admin, S3 Admin)
Role: ExecuteGlueService
```
***
### Carrie Account create role named ExecuteGlueService and trust entity policy as below

```
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "Statement1",
			"Effect": "Allow",
			"Principal": {
				"AWS": [
					"arn:aws:iam::875120157787:role/airflow-role"
				]
			},
			"Action": "sts:AssumeRole"
		}
	]
}
```
***
### Jacky Account create role airflow-role with below inline policy
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Resource": "*"
        }
    ]
}
```
### or specified the resource arn as below

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Resource": "arn:aws:iam::497254257696:role/ExecuteGlueService"
        }
    ]
}
```

### Note: must attach below policy into user cedc in Jacky account so user cedc can attach airflow-role to EC2 instance
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ec2:RunInstances",
                "ec2:AssociateIamInstanceProfile",
                "ec2:ReplaceIamInstanceProfileAssociation"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": "arn:aws:iam::875120157787:role/airflow-role"
        }
    ]
}
```


### Then Go to EC2 Instance in Jacky Account to verify

### create .aws if not exists
```
mkdir ~/.aws/
```
### create config file if not exists
```
touch config
```
### add below profile into config
```
[profile airflow-role]
role_arn=arn:aws:iam::497254257696:role/ExecuteGlueService
credential_source = Ec2InstanceMetadata
```

### verify the instance profile can assume the role
```
aws sts get-caller-identity --profile airflow-role
```

### verify the instance profile can list the data of s3 and glue in Carrie account
```
aws s3 ls --profile airflow-role

aws glue list-jobs --region ap-northeast-1 --profile airflow-role
```

### verify the instance can list s3 bucket in Jacky account without profile
```
aws s3 ls
```