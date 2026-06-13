import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def get_pg_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
        f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
    )

# Map CSV filename to Postgres table name
TABLES = {
    'customer': 'customer',
    'orders': 'orders',
    'lineitem': 'lineitem',
    'supplier': 'supplier',
    'part': 'part'
}

def load_csv_to_postgres(table_name: str, engine):
    filepath = f"./tpch_data/{table_name}.csv"
    print(f"Loading {filepath} into Postgres table '{table_name}'...")
    
    df = pd.read_csv(filepath)
    print(f"  Read {len(df)} rows from CSV")
    print(f"  Columns: {list(df.columns)}")
    
    # if_exists='replace' drops and recreates the table data
    # but keeps the schema we defined
    # we use 'append' since we already created the tables with proper types
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='append',
        index=False
    )
    print(f"  Loaded {len(df)} rows into {table_name}")

if __name__ == "__main__":
    engine = get_pg_engine()
    for table in TABLES:
        load_csv_to_postgres(table, engine)
    print("\nAll tables loaded into Postgres successfully.")