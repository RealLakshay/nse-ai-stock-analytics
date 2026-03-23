docker exec ingestion-service python -c "
import yfinance as yf
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='redpanda:19092',
    value_serializer=lambda v: json.dumps(v).encode()
)

tickers = ['RELIANCE.NS', 'TCS.NS', 'INFY.NS', 'HDFCBANK.NS', 'TATAMOTORS.NS']
for ticker in tickers:
    df = yf.download(ticker, period='30d', interval='1d')
    for ts, row in df.iterrows():
        msg = {
            'ticker': ticker,
            'timestamp': str(ts),
            'open': float(row['Open']),
            'high': float(row['High']),
            'low': float(row['Low']),
            'close': float(row['Close']),
            'volume': int(row['Volume']),
            'source': 'yfinance'
        }
        producer.send('stock.prices.raw', msg)
    print(f'Sent 30 days of data for {ticker}')

producer.flush()
print('Done!')
"