# etl/generate_data.py

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# --- Configuration ---
NUM_CUSTOMERS = 500
NUM_PRODUCTS = 200
NUM_STORES = 10
NUM_SALES = 10000 # Number of transactions to generate
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 12, 31)

# Create output directory if it doesn't exist
os.makedirs('data', exist_ok=True)

# --- 1. Generate Customers ---
print("Generating Customers...")
customer_data = []
for i in range(1, NUM_CUSTOMERS + 1):
    customer_data.append({
        "customer_id": i,
        "first_name": f"Customer_{i}",
        "last_name": f"Lastname_{i}",
        "email": f"customer{i}@example.com",
        "city": random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
        "signup_date": (START_DATE - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
    })

df_customers = pd.DataFrame(customer_data)
df_customers.to_csv('data/customers.csv', index=False)
print(f"Created {len(df_customers)} customers.")

# --- 2. Generate Stores ---
print("Generating Stores...")
store_data = []
for i in range(1, NUM_STORES + 1):
    store_data.append({
        "store_id": i,
        "store_name": f"Store_{i}",
        "city": random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
        "region": random.choice(["North", "South", "East", "West"]),
        "manager_name": f"Manager_{i}"
    })

df_stores = pd.DataFrame(store_data)
df_stores.to_csv('data/stores.csv', index=False)
print(f"Created {len(df_stores)} stores.")

# --- 3. Generate Products ---
print("Generating Products...")
product_categories = ["Electronics", "Clothing", "Home & Garden", "Books", "Toys", "Sports"]
product_data = []
for i in range(1, NUM_PRODUCTS + 1):
    category = random.choice(product_categories)
    base_price = random.uniform(5.00, 200.00)
    # Add some variation based on category
    if category == "Electronics":
        base_price *= random.uniform(1.2, 2.0)
    elif category == "Clothing":
        base_price *= random.uniform(0.5, 1.5)
    
    product_data.append({
        "product_id": i,
        "product_name": f"Product_{i}",
        "category": category,
        "brand": f"Brand_{random.randint(1, 20)}",
        "unit_cost": round(base_price * 0.6, 2), # Cost is 60% of price
        "unit_price": round(base_price, 2)
    })

df_products = pd.DataFrame(product_data)
df_products.to_csv('data/products.csv', index=False)
print(f"Created {len(df_products)} products.")

# --- 4. Generate Sales Transactions ---
print("Generating Sales Transactions...")
sales_data = []
for _ in range(NUM_SALES):
    sale_date = START_DATE + timedelta(days=random.randint(0, (END_DATE - START_DATE).days))
    # Simulate seasonality - higher sales in Q4 (Nov, Dec)
    if sale_date.month in [11, 12]:
        quantity = random.randint(1, 5)
    else:
        quantity = random.randint(1, 3)
    
    sales_data.append({
        "transaction_id": random.randint(100000, 999999), # Unique ID
        "customer_id": random.randint(1, NUM_CUSTOMERS),
        "product_id": random.randint(1, NUM_PRODUCTS),
        "store_id": random.randint(1, NUM_STORES),
        "date": sale_date.strftime('%Y-%m-%d'),
        "quantity": quantity,
        "unit_price": df_products[df_products['product_id'] == product_data[random.randint(0, NUM_PRODUCTS-1)]['product_id']]['unit_price'].iloc[0], # Get price from product table
        "total_amount": 0 # Will calculate after getting price
    })

# Convert to DataFrame
df_sales = pd.DataFrame(sales_data)
# Calculate total amount based on unit_price and quantity
df_sales['total_amount'] = df_sales['unit_price'] * df_sales['quantity']
# Round to 2 decimal places
df_sales['total_amount'] = df_sales['total_amount'].round(2)
# Drop the temporary unit_price column if you don't want it in the final CSV
# df_sales = df_sales.drop(columns=['unit_price'])

df_sales.to_csv('data/sales_transactions.csv', index=False)
print(f"Created {len(df_sales)} sales transactions.")

print("\nAll data generation complete! CSV files saved in the 'data/' folder.")
