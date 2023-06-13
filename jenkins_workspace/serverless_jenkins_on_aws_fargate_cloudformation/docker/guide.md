## copy private key file to .ssh path
```
cp id_rsa.github /home/ec2-user/.ssh/id_rsa
```
## modify the permission of the file
```
chmod 600 /home/ec2-user/.ssh/id_rsa
```
## git config
```
git config --global user.name "jenkins-workstation"
git config --global user.email "sixgod2019@outlook.com"
```
## git clone 
```
git clone git@github.com:SixGod191001/CEDC.git
```
## enter controller path
```
cd CEDC/jenkins_workspace/serverless_jenkins_on_aws_fargate_cloudformation/docker/ecr/controller
```
## build docker with the docker file under the path
```
docker build -t jenkins-ecs-fargate .
```
## create two repositories in AWS ECR, named jenkins-master,jenkins-slave
## 1. Retrieve an authentication token and authenticate your Docker client to your registry.
##    Use the AWS CLIV2:
```
aws ecr get-login-password --region ap-northeast-1 | docker login --username AWS --password-stdin 213903534337.dkr.ecr.ap-northeast-1.amazonaws.com
```

## 2. Build your Docker image using the following command. For information on building a Docker file from scratch see the
##   instructions here . You can skip this step if your image is already built:
```
docker build -t jenkins-master .
```
### 测试以下新image
```
docker run -d -p 8080:8080 jenkins-master
```
## 3. After the build completes, tag your image so you can push the image to this repository:
```   
docker tag jenkins-master:latest 213903534337.dkr.ecr.ap-northeast-1.amazonaws.com/jenkins-master:latest
```

## 4. Run the following command to push this image to your newly created AWS repository:
```
docker push 213903534337.dkr.ecr.ap-northeast-1.amazonaws.com/jenkins-master:latest
```

## 5. Push slave node to ecr,route to the docker/ecr/slave folder
```
aws ecr get-login-password --region ap-northeast-1 | docker login --username AWS --password-stdin 213903534337.dkr.ecr.ap-northeast-1.amazonaws.com

docker build -t jenkins-slave .
docker tag jenkins-slave:latest 213903534337.dkr.ecr.ap-northeast-1.amazonaws.com/jenkins-slave:latest
docker push 213903534337.dkr.ecr.ap-northeast-1.amazonaws.com/jenkins-slave:latest

```