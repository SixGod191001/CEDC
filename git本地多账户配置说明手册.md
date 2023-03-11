## 查看是否配置过git全局变量
    git config --global --list
    
    user.name=YANG YANG
    user.email=SixGod2019@outlook.com
    gui.recentrepo=D:/project/CompareTool
    gui.recentrepo=D:/KnowledgeHub
    gui.recentrepo=D:/project/SharedInfo

## 删除全局变量
    git config --global --unset user.name "YANG YANG"
    git config --global --unset user.email "SixGod2019@outlook.com"

## 生成新的 SSH keys
## GitHub 的钥匙

    ssh-keygen -t rsa -f ~/.ssh/id_rsa.github -C "SixGod2019@github.outlook.com"


## Gitee 的钥匙

    ssh-keygen -t rsa -f ~/.ssh/id_rsa.gitee -C "SixGod2019@gitee.outlook.com"


## 识别 SSH keys 新的私钥
## 默认只读取 id_rsa，为了让 SSH 识别新的私钥，需要将新的私钥加入到 SSH agent 中
    ssh-agent bash
    ssh-add ~/.ssh/id_rsa.github
    ssh-add ~/.ssh/id_rsa.gitee


## 多账号配置 config 文件
## 创建config文件
    touch ~/.ssh/config 

## config 中填写的内容
    #Default gitHub user Self
    Host github.com
    HostName github.com
    User git
    IdentityFile ~/.ssh/id_rsa.github
    
    # gitee
    Host gitee.com
    Port 22
    HostName gitee.com
    User git
    IdentityFile ~/.ssh/id_rsa.gitee
	
	
## 添加 ssh
- https://github.com/settings/keys 所有项目
- https://github.com/SixGod191001/CEDC/settings/keys 单独项目

## 将 id_rsa.github.pub 中的内容填进去，起名的话随意。
- https://gitee.com/profile/sshkeys

## 将 id_rsa.gitee.pub 中的内容填进去，起名的话随意。


## 测试
    ssh -T git@gitee.com
    ssh -T git@github.com


## 配置全局变量
    git config --global user.name "YANG YANG"
    git config --global user.email "SixGod2019@outlook.com"


## 拉取代码
    git clone git@gitee.com:SixGod2019/KnowledgeHub.git "D:\project\KnowledgeHub"
    git clone git@gitee.com:SixGod2019/compare-tool.git "D:\project\compare-tool"
    git clone git@gitee.com:SixGod2019/shared-info.git "D:\project\shared-info"
    git clone git@gitee.com:SixGod2019/template-management.git "D:\project\template-management"
    git clone git@github.com:SixGod191001/CEDC.git "D:\project\CEDC"



