# 如何使用CloudFormation创建AWS上的其他服务

## 1.发布GlueJob

​		(1).首先可以使用aws上的CloudShell服务,或在官网搜索AWSCLI服务，进行下载，安装到本地。

​		(2).配置AWS，在命令行输入awsconfigure，设置aws_access_key_id、aws_secret_access_key、region、output

​		(3).编写对应的Yaml、Json文件，在CloudShell上使用，需要上传到服务器上。然后输入以下命令

```shell
aws cloudformation deploy \
--stack-name devops-glue-template \
--template-file template.yaml \		## 编写的yaml文件地址
--parameter-overrides file://params.json \	## 编写的Json文件地址
--s3-bucket s3BucketName \	## s3 bucket的名字
--s3-prefix devops \
--region ap-northeast-1
```

​		(4).编写对应的Yaml、Json文件，在Windows本地使用。

```shell
aws cloudformation deploy ^
--stack-name devops-glue-template ^
--template-file template.yaml ^
--parameter-overrides file://params.json ^
--s3-bucket s3BucketName ^
--s3-prefix devops ^
--region ap-northeast-1
```

## 2.创建s3_bucket

​		根据cloudformation的官方文档，编写yaml文件，设置参数。

```yaml
Parameters:
  BucketName:
    Type: String
    Description: creat unique S3 bucket name in the world(mandatory change name required)
    Default: xxxxxxs3location
Resources:
  S3Bucket:
    Type: 'AWS::S3::Bucket'
    Description: Creating Amazon S3 bucket from CloudFormation
    DeletionPolicy: Retain
    Properties:
      BucketName: !Ref BucketName   ## 接收Parameters中的BucketName下，Default内的值。！Ref是aws上接收参数的函数。
      AccessControl: Private
Outputs:
  S3Bucket:
    Description: Bucket Created using this template.
    Value: !Ref S3Bucket    ## 同理，接收Resources中 S3Bucket的全部信息，并输出。以便后面调用。
```

## 3.创建数据库

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Description: Creates an empty PostgreSQL RDS database for the Allergan automated deployments.
Parameters:
  PostgresInstanceName:
    NoEcho: 'false'
    Description: RDS PostgreSQL Instance Name
    Type: String
    Default: PostgresRdsDB
    MinLength: '1'
    MaxLength: '63'
    AllowedPattern: "[a-zA-Z][a-zA-Z0-9]*"
  DatabaseUsername:
    AllowedPattern: "[a-zA-Z0-9]+"
    ConstraintDescription: must contain only alphanumeric characters.
    Description: The database admin account user name.
    MaxLength: '16'
    MinLength: '1'
    Default: pgadmin
    Type: String
  DatabasePassword:
    AllowedPattern: "^(?=.*[0-9])(?=.*[a-zA-Z])([a-zA-Z0-9]+)"
    ConstraintDescription: Must contain only alphanumeric characters with at least one capital letter and one number.
    Description: The database admin account password.
    MaxLength: '41'
    MinLength: '8'
    NoEcho: 'true'
    Type: String
    Default: password123
Metadata:
  AWS::CloudFormation::Interface:
    ParameterGroups:
    - Label:
        default: PostgreSQL, admin username and password
      Parameters:
        - PostgresInstanceName
        - DatabaseUsername
        - DatabasePassword
      ParameterLabels:
        PostgresInstanceName:
            default: PostgreSQL
        DatabaseUsername:
            default: pgadmin
        DatabasePassword:
            default: password123
Resources:
  PostgresSecurityGroup:
    Type: AWS::EC2::SecurityGroup
    Properties:
      GroupDescription: PostgreSQL Security Group
      SecurityGroupIngress:
      - IpProtocol: tcp
        FromPort: '5432'
        ToPort: '5432'
        CidrIp: 0.0.0.0/0
  PostgresDatabase:
    Type: AWS::RDS::DBInstance
    Properties:
      VPCSecurityGroups:
      - Fn::GetAtt:
        - PostgresSecurityGroup
        - GroupId
      DBInstanceIdentifier:
        Ref: PostgresInstanceName
      Engine: postgres
      EngineVersion: 13.3
      MultiAZ: false
      DBInstanceClass: db.t3.micro
      AllocatedStorage: '20'
      MasterUsername:
        Ref: DatabaseUsername
      MasterUserPassword:
        Ref: DatabasePassword
      PubliclyAccessible: 'true'
      Tags:
        -
          Key: "Name"
          Value: "postgresmaster"
        -
          Key: "project"
          Value: "development unittest"
      BackupRetentionPeriod: '1'
    DependsOn: PostgresSecurityGroup
Outputs:
   PostgresDatabaseEndpoint:
     Description: Database endpoint
     Value: !Sub "${PostgresDatabase.Endpoint.Address}:${PostgresDatabase.Endpoint.Port}"

  ## yaml文件和json文件可根据官方模板自己编写，传入想要的参数即可创建服务。
```
