import duckdb
import sys

# Memory usage comparison
def compare_memory_usage():
    import psutil
    import pandas as pd
    
    process = psutil.Process()
    
    # Baseline memory
    baseline = process.memory_info().rss / 1024 / 1024
    
    # Load with pandas
    df = pd.read_parquet('stocks.parquet')
    pandas_memory = process.memory_info().rss / 1024 / 1024 - baseline
    
    # Clear pandas data
    del df
    
    # Load with DuckDB
    conn = duckdb.connect(':memory:')
    conn.execute("CREATE TABLE stocks AS SELECT * FROM 'stocks.parquet'")
    duckdb_memory = process.memory_info().rss / 1024 / 1024 - baseline
    
    print(f"Pandas memory usage: {pandas_memory:.1f} MB")
    print(f"DuckDB memory usage: {duckdb_memory:.1f} MB")
    print(f"Memory savings: {(pandas_memory - duckdb_memory) / pandas_memory * 100:.1f}%")

compare_memory_usage()
