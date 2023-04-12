# 本地通过awscli手动测试创建stack的命令行语句
## bat 语法
cd D:\project\KnowledgeHub\etl-framework\prelanding\devops-glue-template\cfn_template

### windows上发布stack
aws cloudformation deploy ^
--stack-name devops-glue-template ^
--template-file template.yaml ^
--parameter-overrides file://params.json ^
--s3-bucket jackyyang-aws-training-code ^
--s3-prefix devops ^
--region ap-northeast-1 

### 删除stack
aws cloudformation delete-stack ^
--stack-name devops-glue-template ^
--region ap-northeast-1 

## bash 语法
### Linux上发布stack
aws cloudformation deploy \
--stack-name devops-glue-template \
--template-file template.yaml \
--parameter-overrides file://params.json \
--s3-bucket jackyyang-aws-training-code \
--s3-prefix devops \
--region ap-northeast-1 