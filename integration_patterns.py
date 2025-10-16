import duckdb
import pandas as pd
import numpy as np

conn = duckdb.connect('financial_analysis.duckdb')

print("=== DuckDB Integration Patterns ===")

# Pattern 1: Direct DataFrame manipulation
print("\\n1. Direct DataFrame Operations")
df = pd.read_parquet('stocks.parquet')
df['Price_Change'] = df['Close'] - df['Open']
df['Price_Change_Pct'] = (df['Close'] - df['Open']) / df['Open'] * 100

# Query the DataFrame directly with DuckDB
result1 = duckdb.sql("""
    SELECT Symbol,
           AVG(Price_Change_Pct) as avg_daily_return,
           STDDEV(Price_Change_Pct) as volatility
    FROM df
    GROUP BY Symbol
    ORDER BY avg_daily_return DESC
    LIMIT 10
""").df()

print("Top performing stocks by average daily return:")
print(result1.to_string())

# Pattern 2: Memory-efficient file processing
print("\\n2. Memory-efficient File Processing")
# Process large files without loading into memory
large_analysis = conn.execute("""
    SELECT 
        Symbol,
        COUNT(*) as trading_days,
        AVG(Volume * Close) as avg_dollar_volume,
        CORR(Volume, Close) as volume_price_correlation
    FROM 'stocks.parquet'
    WHERE Volume > 1000000  -- Filter high-volume days
    GROUP BY Symbol
    HAVING COUNT(*) > 100   -- At least 100 trading days
    ORDER BY avg_dollar_volume DESC
""").df()

print("High-volume trading analysis:")
print(large_analysis.head().to_string())

# Pattern 3: Complex analytical functions
print("\\n3. Complex Analytical Functions")
# Moving averages and technical indicators
technical_analysis = conn.execute("""
    SELECT 
        Symbol,
        Date,
        Close,
        AVG(Close) OVER (
            PARTITION BY Symbol 
            ORDER BY Date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as sma_20,
        AVG(Close) OVER (
            PARTITION BY Symbol 
            ORDER BY Date 
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) as sma_50,
        ROW_NUMBER() OVER (PARTITION BY Symbol ORDER BY Close DESC) as price_rank
    FROM stocks
    WHERE Symbol IN ('AAPL', 'MSFT', 'GOOGL')
    ORDER BY Symbol, Date DESC
""").df()

print("Technical analysis (sample):")
print(technical_analysis.head(10).to_string())

# Pattern 4: Data export and integration
print("\\n4. Data Export Patterns")

# Export to different formats
conn.execute("COPY large_analysis TO 'analysis_results.csv' (HEADER, DELIMITER ',')")
conn.execute("COPY large_analysis TO 'analysis_results.parquet' (FORMAT PARQUET)")

# Create views for repeated analysis
conn.execute("""
    CREATE OR REPLACE VIEW daily_returns AS
    SELECT 
        Symbol,
        Date,
        Close,
        LAG(Close) OVER (PARTITION BY Symbol ORDER BY Date) as prev_close,
        (Close - LAG(Close) OVER (PARTITION BY Symbol ORDER BY Date)) / 
        LAG(Close) OVER (PARTITION BY Symbol ORDER BY Date) * 100 as daily_return
    FROM stocks
    WHERE LAG(Close) OVER (PARTITION BY Symbol ORDER BY Date) IS NOT NULL
""")

# Use the view
volatility_analysis = conn.execute("""
    SELECT 
        Symbol,
        AVG(daily_return) as mean_return,
        STDDEV(daily_return) as volatility,
        MIN(daily_return) as worst_day,
        MAX(daily_return) as best_day
    FROM daily_returns
    GROUP BY Symbol
    ORDER BY volatility DESC
""").df()

print("Volatility analysis:")
print(volatility_analysis.head().to_string())

conn.close()
print("\\nAnalysis complete! Check generated files.")
