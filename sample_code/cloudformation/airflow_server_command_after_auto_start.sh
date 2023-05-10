#!/bin/sh
echo 'run init.sh in /var/lib/cloud/scripts/per-boot'
echo 'sudo su root'su'd
sudo su root
echo 'source /home/ubuntu/venv/bin/activate'
. /home/ubuntu/venv/bin/activate
echo 'cd /root/airflow'
cd /root/airflow
echo 'airflow db init'
airflow db init
airflow webserver & 
airflow scheduler &


