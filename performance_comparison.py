import duckdb
import pandas as pd
import time

# Load data with pandas
print("Loading data with pandas...")
start_time = time.time()
df_pandas = pd.read_parquet('stocks.parquet')
pandas_load_time = time.time() - start_time

# Connect to DuckDB
conn = duckdb.connect('financial_analysis.duckdb')

# Test queries
test_queries = {
    "Daily Volume Analysis": {
        "description": "Calculate average daily volume by sector",
        "sql": """
            SELECT Sector,
                   COUNT(*) as trading_days,
                   AVG(Volume) as avg_volume,
                   SUM(Volume) as total_volume
            FROM stocks
            WHERE Volume > 0
            GROUP BY Sector
            ORDER BY avg_volume DESC
        """,
        "pandas": lambda df: df[df['Volume'] > 0].groupby('Sector').agg({
            'Volume': ['count', 'mean', 'sum']
        }).round(0)
    },
    
    "Price Performance": {
        "description": "Calculate monthly price performance",
        "sql": """
            SELECT Symbol,
                   DATE_TRUNC('month', Date) as month,
                   AVG(Close) as avg_price,
                   (MAX(High) - MIN(Low)) / MIN(Low) * 100 as monthly_volatility
            FROM stocks
            GROUP BY Symbol, DATE_TRUNC('month', Date)
            ORDER BY monthly_volatility DESC
            LIMIT 10
        """,
        "pandas": lambda df: df.groupby(['Symbol', pd.Grouper(key='Date', freq='M')]).agg({
            'Close': 'mean',
            'High': 'max',
            'Low': 'min'
        }).head(10)
    },
    
    "Market Cap Analysis": {
        "description": "Analyze market cap distribution",
        "sql": """
            SELECT 
                CASE 
                    WHEN Market_Cap > 200000000000 THEN 'Large Cap'
                    WHEN Market_Cap > 10000000000 THEN 'Mid Cap'
                    ELSE 'Small Cap'
                END as cap_category,
                COUNT(DISTINCT Symbol) as companies,
                AVG(Close) as avg_stock_price,
                SUM(Volume) as total_volume
            FROM stocks
            WHERE Market_Cap > 0
            GROUP BY cap_category
            ORDER BY companies DESC
        """
    }
}

# Run performance tests
results = []

for query_name, query_info in test_queries.items():
    print(f"\\n=== {query_name} ===")
    print(f"Description: {query_info['description']}")
    
    # DuckDB execution
    start_time = time.time()
    duckdb_result = conn.execute(query_info['sql']).fetchdf()
    duckdb_time = time.time() - start_time
    
    print(f"DuckDB time: {duckdb_time:.4f} seconds")
    print(f"Result shape: {duckdb_result.shape}")
    
    # Pandas execution (if available)
    if 'pandas' in query_info:
        start_time = time.time()
        pandas_result = query_info['pandas'](df_pandas)
        pandas_time = time.time() - start_time
        
        print(f"Pandas time: {pandas_time:.4f} seconds")
        speedup = pandas_time / duckdb_time if duckdb_time > 0 else 0
        print(f"DuckDB speedup: {speedup:.2f}x")
        
        results.append({
            'query': query_name,
            'duckdb_time': duckdb_time,
            'pandas_time': pandas_time,
            'speedup': speedup
        })

# Summary
if results:
    avg_speedup = sum(r['speedup'] for r in results) / len(results)
    print(f"\\n=== Performance Summary ===")
    print(f"Average DuckDB speedup: {avg_speedup:.2f}x")
    
    # Save results
    results_df = pd.DataFrame(results)
    results_df.to_csv('performance_results.csv', index=False)
