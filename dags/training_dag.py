from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import wandb
import random

with DAG(
    dag_id='wandb_hpt_example',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    @task
    def train_trial(learning_rate):
        # Initialize W&B within the task
        run = wandb.init(
            project="airflow-integration",
            config={"learning_rate": learning_rate},
            group="hpt-batch-01"
        )
        
        # Simulate training logic
        accuracy = 1 - (learning_rate * random.random())
        
        # Log to W&B
        run.log({"accuracy": accuracy})
        print(f"Trial with LR {learning_rate} resulted in {accuracy} accuracy")
        
        run.finish()

    # Dynamic Task Mapping: This triggers 3 parallel tasks
    learning_rates = [0.01, 0.05, 0.1]
    train_trial.expand(learning_rate=learning_rates)