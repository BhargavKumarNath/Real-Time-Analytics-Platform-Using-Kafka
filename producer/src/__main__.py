import yaml
import time
import logging
import random
import yfinance as yf
from confluent_kafka import avro
import requests 
from confluent_kafka.avro import AvroProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def load_config(config_path='config/stocks.yaml'):
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {config_path}")
        exit(1)

def load_avro_schema(schema_path='producer/schemas/stock_price.avsc'):
    try:
        with open(schema_path, 'r') as file:
            return avro.loads(file.read())
    except FileNotFoundError:
        logging.error(f"Avro schema file not found at {schema_path}")
        exit(1)

def fetch_stock_data(symbol, session):
    """Fetch a single symbol's data with retry and backoff, using a session."""
    max_retries = 3
    delay = 10  # Start with 10 seconds
    
    for attempt in range(max_retries):
        try:
            ticker = yf.Ticker(symbol, session=session)
            info = ticker.info
            if not info or 'currentPrice' not in info and 'regularMarketPrice' not in info:
                raise ValueError(f"Incomplete data received for {symbol}")
            return info
        except Exception as e:
            logging.warning(f"Attempt {attempt+1}: Error fetching {symbol}: {e}")
            if attempt < max_retries - 1:  
                time.sleep(delay)
                delay *= 2  # 10s, 20s
    return None



def run_producer():
    config = load_config()
    key_schema = load_avro_schema('producer/schemas/stock_price_key.avsc') 
    value_schema = load_avro_schema('producer/schemas/stock_price.avsc')

    producer_config = {
        'bootstrap.servers': config['kafka']['bootstrap_servers'],
        'schema.registry.url': config['kafka']['schema_registry_url']
    }

    producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })

    symbols = config['stocks']
    topic = config['kafka']['topic_name']

    while True:
        for symbol in symbols:
            info = fetch_stock_data(symbol, session)
            
            if not info:
                logging.warning(f"No data for {symbol}. Skipping.")
                continue

            current_price = info.get('currentPrice') or info.get('regularMarketPrice')
            volume = info.get('volume') or info.get('regularMarketVolume')

            if current_price is None or volume is None:
                logging.warning(f"Price or volume not available for {symbol}. Skipping.")
                continue

            message = {
                'timestamp': int(time.time() * 1000),
                'symbol': symbol,
                'price': float(current_price),
                'volume': int(volume)
            }

            key = {'symbol': symbol}

            try:
                producer.produce(topic=topic, key=key, value=message)
                logging.info(f"Produced message for {symbol}: {message}")
            except Exception as e:
                logging.error(f"Failed to produce message for {symbol}: {e}")

            sleep_duration = random.uniform(45, 75)
            logging.info(f"Waiting for {sleep_duration:.2f} seconds before next symbol.")
            time.sleep(sleep_duration)

        producer.flush()
        logging.info("Full batch sent and flushed.")

if __name__ == "__main__":
    import os
    if os.path.exists('/app/config/stocks.yaml'):
        run_producer()
    else:
        os.chdir('../..')
        run_producer()
