# -*- coding: utf-8 -*-
"""
Entry point for the Airflow framework.

This script serves as the main entry point for triggering various tasks in Airflow.
It uses command-line arguments to determine which task to execute and passes parameters
in JSON format to the corresponding module.

Example usage: python main.py --trigger=start_batch --params='{"datasource_name": "sample", "load_type": "ALL", "run_type": "glue",
"glue_template_name":"cedc_sales_prelanding_template"}'

Variable definitions:
- trigger: The method to be called, such as executing, sending emails, monitoring, etc.
- params: Type: JSON string, containing variables such as datasource_name, load_type, run_type, glue_template_name.

Author: YANG
Update: 2023/4/16 - Initial version with callable functions (functions are empty)
        2023/4/21 - Julie added start, notify function calls
        2024/06/20 - Yang refactor all code
"""

import argparse
import json
from airflow_workspace.module.batch_processor import *


def check_trigger(trigger):
    """
    Determines which function to call based on the provided trigger.

    Args:
    - trigger (str): The name of the function to call.

    Returns:
    - function: The corresponding function if found, otherwise a lambda function returning an error message.
    """
    switcher = {
        "dependency_check": dependency_check,
        "start_batch": start_batch,
        "monitor_batch": monitor_batch,
        "batch_notify": batch_notify,
        "trigger_next_dag": trigger_next_dag,
        "start_dag": start_dag,
        "stop_dag": stop_dag,
    }
    return switcher.get(trigger, lambda: "Invalid trigger provided")


if __name__ == "__main__":
    # Default parameters
    default_param = {"task_name": "task_cedc_sales_prelanding_push_params", "dag_id": "dag_cedc_sales_prelanding", "base_url": "http://13-231-176-77:8080"}
    default_param = json.dumps(default_param)

    # Argument parser setup
    parser = argparse.ArgumentParser(description='Get variables from task in Airflow DAG')
    parser.add_argument("--trigger", type=str, default='start_batch', help="The method to call (e.g., start_batch, dependency_check)")
    parser.add_argument("--params", type=str, default=default_param, help="JSON string with parameters for the specified method")

    args = parser.parse_args()

    # Choose the trigger module
    batch = check_trigger(args.trigger)

    # Convert JSON string to dictionary
    batch_event = json.loads(args.params)

    # Execute the corresponding function with parameters
    batch(batch_event)

    # Note: logger methods need to be refactored and extracted (WIP)
    # logger.info(batch(event, "context"))
