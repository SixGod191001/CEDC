import requests
import json
from pprint import pprint
from datetime import datetime
import sched, time

def get_execution_time(): #获取当前时间
    # datetime object containing current date and time
    now = datetime.utcnow()#获取当前世界时间

    print("now =", now)

    dt_string = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    print("date and time =", dt_string)

    return dt_string

dag_id = "cedc_airflow_dag_id"#写死一个dag_id

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
        url=" ",
        data=json.dumps(data),
        headers=header,
        auth=("admin", "admin"))

    pprint(result.content.decode('utf-8'))

    result = json.loads(result.content.decode('utf-8'))

    pprint(result)

    return result


def get_dag_run(dag_run_id):
    result = requests.get(
        url=" ",
        auth=("admin", "admin"))

    pprint(result.content.decode('utf-8'))


    result = json.loads(result.content.decode('utf-8'))

    pprint(result)

    return result

result = trigger_dag()
dag_run_id = result["dag_run_id"]

s = sched.scheduler(time.time, time.sleep)#调度时间
"""
def watch_dag_until_complete():
    result = get_dag_run(dag_run_id)
    state = result["state"]

    if state != "success":
        s.enter(1, 1, watch_dag_until_complete)
    else:
        print("dag completed!")

s.enter(1, 1, watch_dag_until_complete)"""
s.run()
