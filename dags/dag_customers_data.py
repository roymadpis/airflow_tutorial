import configparser
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os, sys


import pandas as pd
import numpy as np
import random
from faker import Faker

# This tells Python to look in /opt/airflow/dags for the 'scripts' package
dag_dir = os.path.dirname(os.path.abspath(__file__))
if dag_dir not in sys.path:
    sys.path.append(dag_dir)
    
        
sys.path.append(os.path.dirname(__file__))

    
from scripts.create_customers_dataset import generate_customer_data
from scripts.stats_on_customers_table import generate_stats_logic
from scripts.predict_value import identify_marketing_targets

# # 1. Load the configuration
# import yaml
# current_dir = os.path.dirname(__file__) # Get the directory of the current script (project_root/dags)
# conf_path = os.path.join(current_dir, '..', 'config', 'config.yaml') # Go up one level and into the config folder
# conf_path = os.path.abspath(conf_path) # Normalize the path (cleans up the '..' for a cleaner string)
# with open(CONFIG_PATH, 'r') as f:
#     config = yaml.safe_load(f)

# 1. Setup Paths
# __file__ is /opt/airflow/dags/dag_using_config.py
# .parent.parent is /opt/airflow/
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "config.ini"
# 2. Load Configuration using configparser
config = configparser.ConfigParser()
if not config.read(CONFIG_PATH):
    raise FileNotFoundError(f"Config file not found at {CONFIG_PATH}")
############################################################################

# 3. Extracting values (with type conversion)
DAG_ID = config['dag_settings']['dag_id']
SCHEDULE = config['dag_settings']['schedule_interval']

CUST_SEC = config['customers_params']
ANALYSIS = config['analysis_params']

force_recreate = CUST_SEC['force_recreate']
n_customers = int(CUST_SEC['number_of_customers'])
dataset_dir = CUST_SEC['datasets_dir']
file_name = CUST_SEC['customer_filename']

age_range = (int(CUST_SEC['age_range_min']), int(CUST_SEC['age_range_max']))

items_list = [i.strip() for i in CUST_SEC['items_list'].split(',')]
purchase_range = (float(CUST_SEC['purchase_value_range_min']), float(CUST_SEC['purchase_value_range_max']))
mult_range = (float(CUST_SEC['total_value_mult_range_min']), float(CUST_SEC['total_value_mult_range_max']))
stats_file_name = CUST_SEC['stats_file_name']


with DAG(
    dag_id=DAG_ID,
    schedule_interval=SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['production', 'generator', 'config-driven']
) as dag:

    create_customers_table = PythonOperator(
        task_id='generate_customer_csv',
        python_callable=generate_customer_data,
        op_kwargs={
            'n': n_customers,
            'dir_name': dataset_dir,
            'file_name': file_name,
            'force_recreate': force_recreate,
            'age_range': age_range,
            'purchase_value_range': purchase_range,
            'items_list': items_list,
            'total_value_mult_range': mult_range,
            'verbose': True
        }
    )

    # You can add a second task here to 'process' the file created by the first
    customers_stats_task = PythonOperator(
        task_id='compute_stats',
        python_callable=generate_stats_logic,
        op_kwargs={
            'dir_name': dataset_dir,
            'file_name': file_name,
            'stats_file_name': stats_file_name
        }
    )

    predict_value_task = PythonOperator(
        task_id='predict_customer_value',
        python_callable=identify_marketing_targets,
        op_kwargs={
            'input_file': "{{ ti.xcom_pull(task_ids='generate_customer_csv') }}",
            'output_path': ANALYSIS['output_predictions_path'],
            'target_item': ANALYSIS['target_item'],
            'min_spend': ANALYSIS['min_spend_threshold'],
            'age_min': ANALYSIS['target_age_min'],
            'age_max': ANALYSIS['target_age_max'],
            'min_seniority': ANALYSIS['target_min_seniority']
        }
    )
create_customers_table >>  customers_stats_task >> predict_value_task



# def process_data_logic(input_p, output_p, batch):
#     print(f"Reading from: {input_p}")
#     print(f"Processing in batches of: {batch}")
#     print(f"Writing to: {output_p}")

# # 2. Use config values to define the DAG
# with DAG(
#     dag_id=config['dag_id'],
#     schedule_interval=config['schedule_interval'],
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     tags=['example', 'config-driven']
# ) as dag:

#     # 3. Pass parameters into tasks
#     process_task = PythonOperator(
#         task_id='process_customer_data',
#         python_callable=process_data_logic,
#         op_kwargs={
#             'input_p': config['params']['input_path'],
#             'output_p': config['params']['output_path'],
#             'batch': config['params']['batch_size']
#         }
#     )