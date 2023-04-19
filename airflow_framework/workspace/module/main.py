# Single point of entry for the ETL FRAMEWORK application
# Below wrapper accepts type of functionality and input json string with the data source details
import json
import sys
import socket

from batch_processor import *
from commons.framework_logger import logger

# Choose between the type of functionality
def check_trigger(trigger):
    switcher = {
        "dependency_check": dependency_check,
       # "kick_off": run_glue,
        "start": start,
        "monitor": monitor_batch,
        "notify": batch_notify,
        "trigger_next_dag": trigger_next_dag
    }
    return switcher.get(trigger, lambda: "Invalid file  type  provided")


# Framework entry point for execution
if __name__ == '__main__':
    logger.info("Started the ETL Framework thread on : " + str(socket.gethostbyaddr(socket.gethostname())[0]))
    trigger = str(sys.argv[1])
    data_source_info = sys.argv[2]
    batch = check_trigger(trigger)
    event = json.loads(data_source_info)
    logger.info(batch(event, "context"))
