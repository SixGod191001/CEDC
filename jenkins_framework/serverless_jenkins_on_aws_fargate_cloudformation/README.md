#  Jenkins PROD Ready 环境搭建指导手册
## 网络配置
### 安装
在cloudformation里运行network.yaml，该stack会搭建以下内容：

| Name | IP | Description |
| :-----| :----- | :----- |
| VPC | 10.0.0.0/16 | DNS hostnames enabled |
| PublicSubnet1 | 10.0.16.0/20 | ALB in this layer  |
| PublicSubnet2 | 10.0.48.0/20 | ALB in this layer   |
| PublicSubnet3 | 10.0.80.0/20 | ALB in this layer   |
| PrivateSubnet1 | 10.0.0.0/20 | Jenkins service in this layer  |
| PrivateSubnet2 | 10.0.32.0/20 | Jenkins service in this layer  |
| PrivateSubnet3 | 10.0.64.0/20 | Jenkins service in this layer |
| Internet Gateway  |   | attached to the public subnets, this is the network’s route to the internet |
| NAT gateway x3  |   | these allow traffic from any services deployed in the private subnets to reach the internet via the internet gateway |
| EIP x3 |   | EIP for each NAT gateway and attach to the Internet Gateway |


### Note
- 测试情况建议使用ecr那套解决方案，省略了NAT gateway 和 EIP并且架在public subnet
- 该环境需要Certification Manager 和 domain in Router53
  1. 在Route53 申请注册domain e.g jackyyang.com
  2. 创建hosted zone e.g jenkins.jackyyang.com
  3. 在hosted zone里配置record name指向到alb e.g jenkins.jackyyang.com  --> dualstack.jenki-loadb-5p9fpzgo1vz-1484443215.ap-northeast-1.elb.amazonaws.com.
  4. Certificates 需要配置到 domain e.g jenkins.jackyyang.com
  5. 部署jenkins ecs时需要以上信息
  6. 部署后通过账号admin登录，密码从secret manager获取
  7. 登录到jenkins后，配置相关密码的环境变量
  8. 登录到Configure System，配置 Labels为 'ecs', # of executors根据需求配置，如测试环境为 5
  



