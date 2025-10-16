# realtime_integration.py
import duckdb
import time
import random

def simulate_realtime_data():
    """Simulate real-time stock price updates"""
    conn = duckdb.connect('realtime.duckdb')
    
    # Create streaming table
    conn.execute("""
        CREATE OR REPLACE TABLE realtime_prices (
            symbol VARCHAR,
            price DECIMAL(10,2),
            volume INTEGER,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']
    base_prices = {'AAPL': 150, 'MSFT': 300, 'GOOGL': 2500, 'AMZN': 3000}
    
    for _ in range(100):  # Simulate 100 updates
        symbol = random.choice(symbols)
        base_price = base_prices[symbol]
        price = base_price * (1 + random.uniform(-0.02, 0.02))  # ±2% variation
        volume = random.randint(1000, 10000)
        
        conn.execute("""
            INSERT INTO realtime_prices (symbol, price, volume)
            VALUES (?, ?, ?)
        """, [symbol, price, volume])
        
        # Calculate running statistics
        if _ % 20 == 0:
            stats = conn.execute("""
                SELECT symbol,
                       COUNT(*) as updates,
                       AVG(price) as avg_price,
                       STDDEV(price) as price_volatility
                FROM realtime_prices
                GROUP BY symbol
            """).df()
            print(f"\\nUpdate {_}: Running statistics")
            print(stats.to_string())
        
        time.sleep(0.1)  # Simulate delay

simulate_realtime_data()
