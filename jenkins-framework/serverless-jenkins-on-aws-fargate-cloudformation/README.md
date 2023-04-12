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



