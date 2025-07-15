import time
import json
from datetime import datetime, timedelta
from functools import wraps
from polygon import RESTClient
from kafka import KafkaProducer
import logging
logging.basicConfig(level=logging.INFO)

from ignores.api_keys import POLYGON_API_KEY


# Define the Polygon Client based on our free tier
class Rate_Limit_Polygon_Client:
    def __init__(self):
        self.client = RESTClient(api_key=POLYGON_API_KEY)
        self.calls_per_minute = 4 # calls
        self.minute_interval = 60.0 # seconds
        self.last_call_time = 0.0

    def rate_limited_call(self, func, *args, **kwargs):
        """Rate-limited API call wrapper 
        -- ensures that we do not perform more than 4 API calls / minute
        1. update the last_call_time for every 4 calls per minute
        2. func represents each API call """

        elapsed = time.time() - self.last_call_time
        
        if elapsed < self.minute_interval:
            time.sleep(self.minute_interval - elapsed)
        print(f'Elapsed time: {elapsed}')

        self.last_call_time = time.time()
        return func(*args, **kwargs)

    def get_aggregates(self, ticker:str, multiplier:int, timespan:str, from_date:datetime, to_date:datetime):
        """Getter of aggregates with rate limiting"""
        return self.rate_limited_call(
                self.client.get_aggs,
                ticker=ticker,
                multiplier=multiplier,
                timespan=timespan,
                from_=from_date,
                to=to_date,
                adjusted=True,
                sort='desc'
        )

class Financial_Data_Producer:
    def __init__(self):
        self.polygon_client = Rate_Limit_Polygon_Client()
        self.producer = KafkaProducer(
            bootstrap_servers = 'kafka:9092', # @ref services:kafka /docker-compose.yml 
            value_serializer = lambda x: json.dumps(x).encode('utf-8'),
            batch_size = 32000, # 32KB batches
            linger_ms = 100, # 100ms batching
            compression_type = 'gzip'
        )
        self.major_tickers = [
            'AAPL', 'AMZN'
        ]
    
    def fetch_hourly_data(self):
        current_time = datetime.now()
        from_date = (current_time - timedelta(hours=1)).strftime('%Y-%m-%d')
        to_date = current_time.strftime('%Y-%m-%d')

        for ticker in self.major_tickers:
            try:
                # Fetch with rate limiting
                aggs = self.polygon_client.get_aggregates(
                    ticker=ticker,
                    multiplier=1,
                    timespan='hour',
                    from_date=from_date,
                    to_date=to_date
                )
                
                # Process and send to Kafka
                for result in aggs['results']:
                    ohlc_data = {
                        'ticker': result['ticker'],
                        'open': result['o'],
                        'high': result['h'],
                        'low': result['l'],
                        'close': result['c'],
                        'volume': result['v'],
                        'vwap': result['vw'],
                        'fetch_time': current_time.isoformat()
                    }
                    
                    self.producer.send('financial_ohlc', ohlc_data)
                    print(f"Logged {ticker} data to Kafka. ")
                    logging.info(f"Sent {ticker} data to Kafka")
                    
            except Exception as e:
                logging.error(f"Failed to fetch {ticker}: {e}")
        self.producer.flush()
        logging.info("Hourly data fetch completed")

client = RESTClient('EkLVqCYnIetXf9n3IDyi7eLYRieM895T')


"""
TESTING API
aggs = []
for a in client.list_aggs(
    "AAPL",
    1,
    "day",
    "2025-01-09",
    "2025-02-10",
    adjusted="true",
    sort="asc",
):
    aggs.append(a)

print(aggs)
"""