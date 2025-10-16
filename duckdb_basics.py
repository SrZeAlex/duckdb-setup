import duckdb
import pandas as pd
import time

# Connect to DuckDB
conn = duckdb.connect('financial_analysis.duckdb')

print("=== DuckDB Basic Operations ===")

# 1. Load data directly from file
print("\\n1. Loading data from Parquet file...")
start_time = time.time()
conn.execute("CREATE OR REPLACE TABLE stocks AS SELECT * FROM 'stocks.parquet'")
load_time = time.time() - start_time
print(f"Loaded data in {load_time:.3f} seconds")

# 2. Basic data exploration
print("\\n2. Basic data exploration...")
row_count = conn.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
print(f"Total records: {row_count:,}")

date_range = conn.execute("""
    SELECT MIN(Date) as start_date, MAX(Date) as end_date 
    FROM stocks
""").fetchone()
print(f"Date range: {date_range[0]} to {date_range[1]}")

symbols_count = conn.execute("SELECT COUNT(DISTINCT Symbol) FROM stocks").fetchone()[0]
print(f"Unique symbols: {symbols_count}")

# 3. Show table schema
print("\\n3. Table schema:")
schema = conn.execute("DESCRIBE stocks").fetchall()
for col in schema:
    print(f"  {col[0]}: {col[1]}")

# 4. Sample data
print("\\n4. Sample records:")
sample = conn.execute("SELECT * FROM stocks LIMIT 5").fetchdf()
print(sample.to_string())
