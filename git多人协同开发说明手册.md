# Git 多人协同开发避免冲突说明
## 前提条件
- 本地安装git https://git-scm.com/downloads
- 本地有pycharm IDE

## 配置ssh
- 从ADMIN 获取github的ssh private key (id_rsa.github)
- 将private key(id_rsa.github) 复制到 C:\Users\user_name\.ssh 如果没有该目录创建即可

## 拉取项目代码到本地
git clone git@github.com:SixGod191001/CEDC.git "D:\project\CEDC"

## 创建自己的branch
- 在Pycharm中打开项目
- 点击 Git --> Branches
- 点击Local Branches --> main --> New Branch from 'main' 创建自己的分支(默认会自动checkout到该分支)
- 在该分支开发代码
- 提交代码到自己的分支： 点击commit 选择要提交的文件，点击commit and push, 然后按照提示确认提交
- 回到github 网页你会看到提示 xxx had recent pushes about xxx mins ago
- 在github页面点击compare & pull request
- 按照提示merge 如有冲突请按提示解决
- merge后如无需继续开发可以点击Delete branch删除你的分支
- 如保留该分支，则下次开发前需要从main分支pull最新的代码到你的分支




