from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import dag, task
from datetime import datetime
import itertools
import yaml
from pathlib import Path
import csv

FILE_PATH_EXPERIMENTS = "config/experiments.yaml"
PARAM_KEY_SWEEP = "sweep_parameters"
DAG_TO_RUN = "pipeline_with_config"
LOG_EXPERIMENTS_PATH = "datasets/experiment_history.csv"

# Move the logic into a decorated task
@task
def get_sweep_configs(file_path="config/experiments.yaml", param_key="sweep_parameters"):
    # Path resolution (Keep this inside the task)
    base_dir = Path(__file__).resolve().parent.parent
    yaml_path = base_dir / file_path
    
    if not yaml_path.exists():
        raise FileNotFoundError(f"Experiment file not found at {yaml_path}")

    with open(yaml_path, 'r') as f:
        experiment_data = yaml.safe_load(f)
    
    sweep_params = experiment_data.get(param_key, {})
    experiment_id = experiment_data.get('experiment_id', 'default_exp')
    
    if not sweep_params:
        raise ValueError(f"No '{param_key}' found in the YAML file.")

    # Cartesian Product
    keys, values = zip(*sweep_params.items())
    combinations = [
        dict(zip(keys, v)) for v in itertools.product(*values)
    ]
    
    for combo in combinations:
        combo['experiment_id'] = experiment_id

    return combinations

@task
def log_experiment_results(configs, LOG_EXPERIMENTS_PATH):
    # Setup path to your results log
    base_dir = Path(__file__).resolve().parent.parent
    log_path = base_dir / LOG_EXPERIMENTS_PATH
    
    # Ensure the file exists with headers
    file_exists = log_path.exists()
    
    with open(log_path, mode='a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['timestamp', 'experiment_id', 'params', 'status'])
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # We record what we attempted to run
        for config in configs:
            exp_id = config.get('experiment_id', 'unknown')
            # Extract params excluding the ID
            params = {k: v for k, v in config.items() if k != 'experiment_id'}
            
            writer.writerow([timestamp, exp_id, str(params), "Triggered"])
            
    print(f"Results logged to {log_path}")


@dag(
    schedule=None, 
    start_date=datetime(2026, 1, 1), 
    tags=['experiment-controller'],
    render_template_as_native_obj=True # Important for passing dicts correctly
)
def experiment_controller():

    # 1. Call the task to get the list of configs
    configs = get_sweep_configs(file_path=FILE_PATH_EXPERIMENTS, param_key=PARAM_KEY_SWEEP)
    
    # 2. Map the trigger operator over that list
    trigger_experiments = TriggerDagRunOperator.partial(
        task_id="trigger_runs",
        trigger_dag_id=DAG_TO_RUN,
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30
    ).expand(
        conf=configs # Airflow handles the expansion here at runtime
    )

    log_experiment_results(configs, LOG_EXPERIMENTS_PATH) >> trigger_experiments
experiment_controller()
