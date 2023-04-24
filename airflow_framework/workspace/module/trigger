import requests
import json
from pprint import pprint
from datetime import datetime
import sched, time
#获取当前时间
def get_execution_time():
    # datetime object containing current date and time
    now = datetime.utcnow()

    print("now =", now)

    dt_string = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    print("date and time =", dt_string)

    return dt_string

dag_id = "cedc_airflow_id"#写死一个dag_id

def trigger_dag():
    exec_time = get_execution_time()

    data = {
        # "dag_run_id": dag_run_id,
        "execution_date": exec_time,
        # "execution_date": None,
        # "state": None,
        "conf": { }
    }

    header = {"content-type": "application/json"}

    result = requests.post(
        url="",#API接口
        data=json.dumps(data),
        headers=header,
        auth=("admin", "admin"))#用户名密码

    pprint(result.content.decode('utf-8'))

    result = json.loads(result.content.decode('utf-8'))

    pprint(result)

    return result

