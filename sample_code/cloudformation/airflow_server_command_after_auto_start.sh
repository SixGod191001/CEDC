#!/bin/sh
echo 'run init.sh in /var/lib/cloud/scripts/per-boot'
echo 'sudo su root'
sudo su root
echo 'source /home/ubuntu/venv/bin/activate'
. /home/ubuntu/venv/bin/activate
echo 'cd /root/airflow'
# shellcheck disable=SC2164
cd /root/airflow
echo 'airflow db init'
airflow db init
airflow webserver &
airflow scheduler &


