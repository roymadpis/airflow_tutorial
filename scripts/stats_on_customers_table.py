
import os
from pathlib import Path
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

def generate_stats_logic(dir_name, file_name, stats_file_name = "customer_summary_stats.csv"):
    # Locate the file in the home directory
    full_path = Path.home() / dir_name / file_name
    df = pd.read_csv(full_path)
    
    # Using your EXACT column names from the generate function
    stats = {
        "Metric": [
            "Total Customers", 
            "Total Revenue (Total Value Sum)", 
            "Average Customer Age", 
            "Mean Last Purchase Value",
            "Max Purchase Value",
            "Avg Total Value per Customer"
        ],
        "Value": [
            len(df),
            df['total_value'].sum(),
            df['customer_age'].mean(),
            df['last_purchase_value'].mean(),
            df['last_purchase_value'].max(),
            df['total_value'].mean()
        ]
    }
    
    stats_df = pd.DataFrame(stats)
    
    # Print to Airflow logs
    print("--- CUSTOMER DATA SUMMARY ---")
    print(stats_df.to_string(index=False))
    
    # Save the stats file alongside the raw data
    stats_output = Path.home() / dir_name / stats_file_name
    stats_df.to_csv(stats_output, index=False)
    
    return str(stats_output)