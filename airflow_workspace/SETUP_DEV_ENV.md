# Configure Cloud9 for Git Operations with SSH Key

## Step 1: Configure Git User Information

First, configure global Git username and email address:

```sh
git config --global user.name "YANG YANG"
git config --global user.email "SixGod2019@outlook.com"
```

## Step 2: Configure SSH Key
Start SSH agent and add the private key:

```sh
ssh-agent bash
chmod 400 id_rsa.github
ssh-add /home/ec2-user/environment/id_rsa.github
```

## Step 3: Configure SSH Client
Edit SSH configuration file to add GitHub configuration:
vim ~/.ssh/config 
```sh
# Default gitHub user Self
Host github.com
    HostName github.com
    User git
    IdentityFile /home/ec2-user/environment/id_rsa.github
```
Note: Ensure IdentityFile uses the absolute path.


## Step 4: Clone GitHub Repository
Clone the specified GitHub repository using SSH:

```sh
git clone git@github.com:SixGod191001/CEDC.git
```

# Install airflow on cloud9

```sh
pip3 install virtualenv

virtualenv airflow_env
source airflow_env/bin/activate

export AIRFLOW_HOME=~/airflow

pip install -r /home/ec2-user/environment/CEDC/airflow_workspace/requirements.txt

airflow db migrate

airflow webserver --port 8080
```

#nohup airflow webserver --port 8080 &
#ps aux | grep 'airflow webserver'
#kill 12345

## Open another terminal
```
source airflow_env/bin/activate
airflow scheduler
```

## Visit website
http://localhost:8080 or http://<public ip>:8080

Note: Open security group inbound role

## Create admin user
```
airflow users create \
    --username admin \
    --firstname Yang \
    --lastname Yang \
    --role Admin \
    --email SixGod2019@outlook.com \
    --password Welcome01
```