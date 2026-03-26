from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random
def process_data():
    import pandas as pd
    # Imagine this is fetching from your Kafka stream
    data = {'metric': ['cpu', 'mem'], 'value': [10, 20]}
    df = pd.DataFrame(data)
    print(f"Processed {len(df)} rows of data.")

def generate_num():
    random_number = random.randint(1, 10)
    print(f"Random Number: {random_number}")
    return random_number

def use_random_num(ti):
    random_number = ti.xcom_pull(task_ids='generate_random_number')

    if random_number is None:
        raise ValueError("No random number received from XCom")

    print(f"Using Random Number: {random_number}")
    another_random_number = random.randint(1, 100)
    manipulation = random_number + another_random_number
    print(f"Manipulation: {manipulation}")

    return manipulation

with DAG(
    dag_id='hello_roy_dag_new',
    start_date=datetime(2026, 3, 25),
    schedule_interval='@hourly',
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='simulate_data_processing',
        python_callable=process_data
    )
    
    task2 = PythonOperator(
        task_id='generate_random_number',
        python_callable=generate_num
    )    
    
    
    task3 = PythonOperator(
        task_id='using_random_number',
        python_callable=use_random_num
    )
    
    
    # Set dependencies
    task1 >> task2 >> task3