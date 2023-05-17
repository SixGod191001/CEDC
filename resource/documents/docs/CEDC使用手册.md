# CEDC使用手册

## 一、初步准备（建议使用自己电脑）

### 1.注册帐号

#### （1）AWS账号注册

##### [aws官网](https://portal.aws.amazon.com/billing/signup#/start/email)

##### [aws注册教程（无信用卡版）](https://aws.tcloudpower.com/?id=12)

##### [aws注册教程（有信用卡版）](https://blog.csdn.net/jiuhebaobao/article/details/126726475?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522168257490216800188555866%2522%252C%2522scm%2522%253A%252220140713.130102334..%2522%257D&request_id=168257490216800188555866&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~all~baidu_landing_v2~default-4-126726475-null-null.142^v86^insert_down1,239^v2^insert_chatgpt&utm_term=aws%E6%B3%A8%E5%86%8C&spm=1018.2226.3001.4187)

#### （2）Github账号注册

##### [GitHub官网](https://github.com/)

###### 注册完成后将Gitee帐号发送给Carrie等待加入项目群组

#### （3）Gitee账号注册

##### [Gitee官网](https://gitee.com/)

### 2.软件下载

#### （1）python（3..以上版本）

##### [python官网](https://www.python.org/downloads/)

##### [python安装教程](https://blog.csdn.net/qq_45502336/article/details/109531599?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522168254488516800192266658%2522%252C%2522scm%2522%253A%252220140713.130102334..%2522%257D&request_id=168254488516800192266658&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~all~top_positive~default-2-109531599-null-null.142^v86^insert_down1,239^v2^insert_chatgpt&utm_term=python%E5%AE%89%E8%A3%85&spm=1018.2226.3001.4187)

#### （2）pycharm

##### [pycharm官网](https://www.jetbrains.com/pycharm/download/#section=windows)

##### [pycharm安装教程](https://blog.csdn.net/weixin_46211269/article/details/119934323?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522168255595116800217275524%2522%252C%2522scm%2522%253A%252220140713.130102334..%2522%257D&request_id=168255595116800217275524&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~all~top_positive~default-1-119934323-null-null.142^v86^insert_down1,239^v2^insert_chatgpt&utm_term=pycharm%E5%AE%89%E8%A3%85%E6%95%99%E7%A8%8B&spm=1018.2226.3001.4187)

#### （3）notepad++

##### [notepad++官网（可能无法访问）](https://link.csdn.net/?target=https%3A%2F%2Fnotepad-plus-plus.org%2F)

##### [notepad++安装教程](https://blog.csdn.net/qq_44111805/article/details/127822507?ops_request_misc=&request_id=&biz_id=102&utm_term=notepad++%E5%AE%89%E8%A3%85&utm_medium=distribute.pc_search_result.none-task-blog-2~all~sobaiduweb~default-1-127822507.142^v86^insert_down1,239^v2^insert_chatgpt&spm=1018.2226.3001.4187)

#### （4）winscp

##### [winscp官网](https://winscp.net/eng/download.php)

##### [winscp安装教程](https://blog.csdn.net/qq_26383975/article/details/120220823?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522168257881316800184158985%2522%252C%2522scm%2522%253A%252220140713.130102334..%2522%257D&request_id=168257881316800184158985&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~all~top_positive~default-1-120220823-null-null.142^v86^insert_down1,239^v2^insert_chatgpt&utm_term=winscp%E4%B8%8B%E8%BD%BD&spm=1018.2226.3001.4187)

#### （5）git

##### [git官网](https://git-scm.com/)

##### [git安装教程](https://blog.csdn.net/mukes/article/details/115693833?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522168264147316800211519102%2522%252C%2522scm%2522%253A%252220140713.130102334..%2522%257D&request_id=168264147316800211519102&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~all~top_positive~default-1-115693833-null-null.142^v86^insert_down1,239^v2^insert_chatgpt&utm_term=git%E5%AE%89%E8%A3%85&spm=1018.2226.3001.4187)

#### （6）DBeaver

##### [DBeaver官网](https://dbeaver.io/)

##### [DBeaver安装教程](https://blog.csdn.net/weixin_48053866/article/details/125815498?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522168264315516800215097087%2522%252C%2522scm%2522%253A%252220140713.130102334..%2522%257D&request_id=168264315516800215097087&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~all~top_click~default-1-125815498-null-null.142^v86^insert_down1,239^v2^insert_chatgpt&utm_term=DBeaver%E5%AE%89%E8%A3%85&spm=1018.2226.3001.4187)

#### （7）aws命令行

##### [aws命令行配置](https://kdocs.cn/l/ctN7A08Lm6QX)

#### （8）Docker Desktopi（暂时不需要安装）

##### [Docker Desktop官网](https://www.docker.com/)

##### [Docker Desktop安装教程](https://blog.csdn.net/weixin_42222436/article/details/125945225?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522168264326816800226548780%2522%252C%2522scm%2522%253A%252220140713.130102334..%2522%257D&request_id=168264326816800226548780&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~all~top_positive~default-1-125945225-null-null.142^v86^insert_down1,239^v2^insert_chatgpt&utm_term=docker%20desktop%E5%AE%89%E8%A3%85%E6%95%99%E7%A8%8B&spm=1018.2226.3001.4187)



#### *上述提到的七个软件已上传至网盘 链接如下

百度网盘链接:[点击跳转网盘](https://pan.baidu.com/s/1CCsD2U8n2mTWwBq12uODqg)

提取码：matc 









