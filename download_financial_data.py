import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def download_stock_data(symbols, period='2y'):
    """Download stock data for multiple symbols"""
    all_data = []
    
    for symbol in symbols:
        try:
            # Download data
            stock = yf.Ticker(symbol)
            hist = stock.history(period=period)
            
            # Reset index to make Date a column
            hist.reset_index(inplace=True)
            hist['Symbol'] = symbol
            
            # Get company info
            info = stock.info
            hist['Company'] = info.get('longName', symbol)
            hist['Sector'] = info.get('sector', 'Unknown')
            hist['Market_Cap'] = info.get('marketCap', 0)
            
            all_data.append(hist)
            print(f"Downloaded data for {symbol}")
            
        except Exception as e:
            print(f"Error downloading {symbol}: {e}")
    
    # Combine all data
    combined_data = pd.concat(all_data, ignore_index=True)
    return combined_data

# S&P 500 representative stocks
symbols = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA',  # Tech giants
    'JPM', 'BAC', 'WFC', 'GS',                 # Financial services
    'JNJ', 'PFE', 'UNH', 'ABBV',              # Healthcare
    'XOM', 'CVX',                              # Energy
    'WMT', 'PG', 'KO', 'PEP',                 # Consumer goods
    'V', 'MA', 'PYPL'                         # Payment processors
]

print("Downloading stock data...")
stock_data = download_stock_data(symbols)
print(f"Downloaded {len(stock_data)} records for {len(symbols)} stocks")

# Save data
stock_data.to_csv('stocks.csv', index=False)
stock_data.to_parquet('stocks.parquet', index=False)
print("Data saved to stocks.csv and stocks.parquet")
