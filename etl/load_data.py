# etl/load_data.py

import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus # To handle special characters in password if needed

# --- Configuration ---

DB_USER = 'postgres' 
DB_PASSWORD = 'password'
DB_HOST = 'localhost'      # Or your server address
DB_PORT = '5432'           # Default PostgreSQL port
DB_NAME = 'retail_analytics' # Use the name of the database you created

# Construct the connection string for SQLAlchemy
# Using quote_plus is good practice if your password has special characters
password_encoded = quote_plus(DB_PASSWORD)
DATABASE_URL = f'postgresql://{DB_USER}:{password_encoded}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)

def load_data_to_db():
    """
    Loads data from CSV files into the PostgreSQL database.
    Performs necessary transformations, including profit calculation.
    """
    print("Starting ETL process...")

    # --- Load Dimension Tables ---

    # 1. Load Customers
    print("Loading Customers...")
    df_customers = pd.read_csv('../data/customers.csv')
    df_customers.to_sql('dim_customer', con=engine, if_exists='replace', index=False) # 'replace' drops and recreates table
    print(f"Loaded {len(df_customers)} customers into dim_customer.")

    # 2. Load Stores
    print("Loading Stores...")
    df_stores = pd.read_csv('../data/stores.csv')
    df_stores.to_sql('dim_store', con=engine, if_exists='replace', index=False)
    print(f"Loaded {len(df_stores)} stores into dim_store.")

    # 3. Load Products
    print("Loading Products...")
    df_products = pd.read_csv('../data/products.csv')
    df_products.to_sql('dim_product', con=engine, if_exists='replace', index=False)
    print(f"Loaded {len(df_products)} products into dim_product.")

    # --- Load Fact Table with Transformation ---

    # 4. Load Sales Transactions and Calculate Profit
    print("Loading Sales Transactions and Calculating Profit...")
    df_sales = pd.read_csv('../data/sales_transactions.csv')

    # Join sales data with product data to get the unit cost for profit calculation
    # This mimics what might happen in a real ETL pipeline
    df_sales_with_cost = df_sales.merge(
        df_products[['product_id', 'unit_cost']], 
        on='product_id', 
        how='left'
    )

    # Calculate profit: Total Revenue - Total Cost
    df_sales_with_cost['profit'] = (
        df_sales_with_cost['total_amount'] - 
        (df_sales_with_cost['quantity'] * df_sales_with_cost['unit_cost'])
    ).round(2)

    # Select only the columns needed for the fact table
    # Make sure column names match the schema.sql definition
    df_sales_fact = df_sales_with_cost[['transaction_id', 'customer_id', 'product_id', 'store_id', 'date', 'quantity', 'unit_price', 'total_amount', 'profit']].copy()
    df_sales_fact.rename(columns={'date': 'sale_date'}, inplace=True) # Ensure column name matches schema

    # Load the final fact table data into the database
    df_sales_fact.to_sql('sales_fact', con=engine, if_exists='replace', index=False)
    print(f"Loaded {len(df_sales_fact)} sales records into sales_fact, including profit calculation.")

    print("\nETL process completed successfully! Data is now in the PostgreSQL database.")


if __name__ == "__main__":
    load_data_to_db()
