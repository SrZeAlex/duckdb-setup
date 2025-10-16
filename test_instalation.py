import duckdb
import pandas as pd

print(f"DuckDB version: {duckdb.__version__}")
print(f"Pandas version: {pd.__version__}")

# Test basic functionality
conn = duckdb.connect(':memory:')
result = conn.execute("SELECT 'Hello DuckDB!' as message").fetchone()
print(f"Test result: {result[0]}")
conn.close()
