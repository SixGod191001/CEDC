## 先决条件：
AWS Linux 2 有Docker

## AWS Linux2 上安装Docker
- 更新实例上已安装的程序包和程序包缓存
`sudo yum update -y`

- 安装最新的 Docker 引擎软件包
`sudo amazon-linux-extras install docker -y`

- 启动 Docker 服务
`sudo service docker start`

- （可选）要确保 Docker 守护程序在每次系统重新启动后启动，请运行以下命令：
`sudo systemctl enable docker`


- 将 添加到组中，以便无需使用 即可执行 Docker 命令。ec2-userdockersudo
`sudo usermod -a -G docker ec2-user`

- 注销并重新登录以选取新的组权限。您可以通过关闭当前 SSH 终端窗口并在新窗口中重新连接到实例来实现此目的。您的新 SSH 会话将具有相应的组权限。dockerdocker

- 验证是否可以在没有 sudo 的情况下运行 Docker 命令。

- https://docs.aws.amazon.com/AmazonECS/latest/developerguide/create-container-image.html



## Docker 常用命令
### docker 搜索image
`docker search jenkins`

### 获取image
`docker pull jenkins/jenkins`

### 查看本地image
`docker images`

### 运行image
1. -p 80:80 选项将容器上公开的端口 80 映射到主机系统上的端口 80
2. -i: 以交互模式运行容器，通常与 -t 同时使用
3. -t: 为容器重新分配一个伪输入终端，通常与 -i 同时使用；

#### 前台模式（交互式模式）
`docker run -t -i -p 8080:8080 jenkins/jenkins`

#### 后台模式（上线模式）
`docker run -d -p 8080:8080 jenkins/jenkins`

### 查看当前正在运行的image
```bash
docker ps // 查看所有正在运行容器
docker stop containerId // containerId 是容器的ID
```

### 通过命令行和容器交互式沟通
`docker run -it --user root jenkins/jenkins sh`
`docker run -it --user root jenkins/inbound-agent sh`

`docker exec -it --user root <container_id>  /bin/bash`
#### 查看docker操作系统
`cat /etc/issue`

#### 查看宿主机操作系统
```bash
cat /process/version 
# 或者
uname -a
```

#### 例子：安装需要的软件
```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

#### 停止容器
```bash
docker stop $(docker ps -a -q)   // stop停止所有容器
docker rm $(docker ps -a -q)   // remove删除所有容器
```

## docker image删除
### 查看容器残留物
`docker ps -a`

### docker 删除残留物
```bash
docker rm cfa62b6d6f1e db6c07428d16 ca351d8fc400
# 或者
docker container prune
```

### docker 删除不需要的image
`docker rmi <image id>`

## 创建新的docker
`docker build -t jenkins-ecs-fargate .`

### 测试以下新image
`docker run -d -p 8080:8080 jenkins-ecs-fargate`

## 编辑jenkins.yaml 添加configuration as code识别的更多信息
- 账号密码等


## 登录docker hub
`docker login -u jackyyang001`

## 编辑tag
`docker tag jenkins-ecs-fargate jackyyang001/jenkins-ecs-fargate:v2`
`docker tag inbound-agent-with-awscliv2 jackyyang001/inbound-agent-with-awscliv2:v2`


## 上传镜像
`docker push jackyyang001/jenkins-ecs-fargate:v1`


## 编辑上传slave node
```bash
docker build -t inbound-agent-with-awscliv2 .
docker tag inbound-agent-with-awscliv2 jackyyang001/inbound-agent-with-awscliv2:v2
docker push jackyyang001/inbound-agent-with-awscliv2:v2
```