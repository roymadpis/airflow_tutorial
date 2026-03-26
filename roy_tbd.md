
Goal: build a warpper that is able to run a given airlow process multiple times, each time with different variables values, record some metrics and snapshots of logs from the process.


1. Add wandb to the logic of dag_customers_data
we would like to save the outputs of predict_value with the variables values

2. Add the "for loop" logic --> we would like to run multiple parametrs values and record for each run in airflow the outcomes.