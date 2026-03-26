from airflow.decorators import dag, task
from datetime import datetime
import random
import wandb

@dag(
    dag_id='hello_roy_dag_with_wandb',
    start_date=datetime(2026, 3, 25),
    schedule='@hourly',
    catchup=False
)
def my_dag():

    @task
    def process_data():
        import pandas as pd
        data = {'metric': ['cpu', 'mem'], 'value': [10, 20]}
        print(f"Processed {len(data['metric'])} rows")

    @task
    def generate_num():
        num = random.randint(1, 10)
        print(f"Generated: {num}")
        return num

    @task
    def use_random_num(num):
        another = random.randint(1, 100)
        result = num + another
        print(f"Result: {result}")
        return result

    # ✅ This is the correct mapped task
    @task
    def train_trial(learning_rate, random_number):
        run = wandb.init(
            project="airflow-integration",
            config={"learning_rate": learning_rate},
            reinit=True  # IMPORTANT for Airflow
        )

        accuracy = 1 - (learning_rate * random.random())
            
        wandb.log({
            "accuracy": accuracy,
            "random_number": random_number
        })

        print(f"LR {learning_rate} → accuracy {accuracy}, random={random_number}")

        wandb.finish()


    @task
    def generate_random_for_trials(num_trials):
        return [random.randint(1, 100) for _ in range(num_trials)]


    # Flow
    data = process_data()
    num = generate_num()
    result_random_number = use_random_num(num)

    # ✅ Dynamic task mapping (parallel execution)
    learning_rates = [0.01, 0.05, 0.1]
    random_numbers = generate_random_for_trials(num_trials=len(learning_rates))

    train_trial.expand(learning_rate=learning_rates,  random_number=random_numbers)

dag = my_dag()