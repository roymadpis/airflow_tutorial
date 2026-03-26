from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def process_data():
    import pandas as pd
    # Imagine this is fetching from your Kafka stream
    data = {'metric': ['cpu', 'mem'], 'value': [10, 20]}
    df = pd.DataFrame(data)
    print(f"Processed {len(df)} rows of data.")

with DAG(
    dag_id='hello_roy_dag',
    start_date=datetime(2026, 3, 25),
    schedule_interval='@hourly',
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='simulate_data_processing',
        python_callable=process_data
    )