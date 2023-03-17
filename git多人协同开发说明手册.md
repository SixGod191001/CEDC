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
1. 在Pycharm中打开项目
2. 点击 Git --> Branches
3. 点击Local Branches --> main --> New Branch from 'main' 创建自己的分支(默认会自动checkout到该分支)
4. 在该分支开发代码
5. 开发完成后，可以提交代码到自己的branch
6. 提交代码到自己的分支： 点击commit 选择要提交的文件，点击commit and push, 然后按照提示确认提交
7. 回到github 网页你会看到提示 xxx had recent pushes about xxx mins ago
8. pull你的分支到main分支之前需要将main分支最新代码merge到你的分支
9. 进入你的分支，会有提示提醒你你的分支超前还是落后于main分支几个commit
   - 比如  `This branch is 4 commits behind main.`
10. 点进上边提醒那句话就会看到当前分支和main分支的差异。我们要做的就是在提交自己的代码到main 分支之前拉取一版最新的main分支的代码到你的分支。因为有可能别的同事在你merge之前做了merge到main分支的操作。
11. 在github页面点击compare & pull request (最好做个code review然后再确认merge)
12. 按照提示merge 如有冲突请按提示解决
13. merge后如无需继续开发可以点击Delete branch删除你的分支
14. 如保留该分支，则下次开发前需要从main分支pull最新的代码到你的分支

**Note: 我们提交远程仓库前必须先pull再push**




