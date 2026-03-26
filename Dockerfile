FROM apache/airflow:2.8.1-python3.10

## a custom image that includes the wandb library and any ML frameworks (like scikit-learn or torch) we plan to use.
# Install ML libraries and W&B
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    "typing_extensions>=4.10.0" \
    wandb \
    scikit-learn \
    pandas \
    numpy \
    wandb \
    apache-airflow \
    faker