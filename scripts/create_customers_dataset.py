import os
from pathlib import Path
import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime, timedelta


def generate_customer_data(
    n=10, 
    dir_name = "datasets",
    file_name ="customers.csv",
    age_range=(18, 80),
    items_list=["Laptop", "Mouse", "Keyboard", "Monitor", "Phone", "Desk Lamp"],
    purchase_value_range=(10.0, 2000.0),
    total_value_mult_range=(1.5, 10.0) # total_value will be last_purchase * multiplier
):
    
    # 1. Resolve the Home Directory path and create it
    # Path.home() translates to /home/username or C:\Users\username
    home_path = Path.home() / dir_name
    os.makedirs(home_path, exist_ok=True)
    
    full_path = home_path / file_name
    
    fake = Faker() 
    customers = []

    for _ in range(n):
        # 1. Generate core identity data
        first_name = fake.first_name()
        last_name = fake.last_name()
        
        # 2. Randomize numeric and date values
        last_purchase_val = round(random.uniform(*purchase_value_range), 2)
        age = random.randint(*age_range)
        
        # 3. Create a random list of items (between 1 and 4 items)
        num_items = random.randint(1, 4)
        items = random.sample(items_list, num_items)
        
        customer = {
            "customer_id": fake.unique.random_int(min=1000, max=9999),
            "customer_full_name": f"{first_name} {last_name}",
            "customer_phone_number": fake.phone_number(),
            "customer_email": f"{first_name.lower()}.{last_name.lower()}@{fake.free_email_domain()}",
            "customer_age": age,
            "customer_created_ts": fake.date_time_between(start_date='-3y', end_date='now'),
            "last_purchase_items": "|".join(items),
            "last_purchase_value": last_purchase_val,
            "total_value": round(last_purchase_val * random.uniform(*total_value_mult_range), 2)
        }
        customers.append(customer)

    df = pd.DataFrame(customers)
    df.to_csv(full_path, index=False)
    print(f"Generated {n} customers at {full_path}")
    
    return full_path ## Return the path so downstream Airflow tasks know exactly where it is


