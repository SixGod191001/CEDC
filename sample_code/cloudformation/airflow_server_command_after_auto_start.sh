sudo su root
source /home/ubuntu/venv/bin/activate
cd /root/airflow
airflow db init
airflow webserver & 
airflow scheduler &


