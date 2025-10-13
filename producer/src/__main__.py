import yaml
import time
import logging
import yfinance as yf
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# Configuration Loading
def load_config(config_path='config/stocks.yaml'):
    """Loads configuration from a YAML file."""
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {config_path}")
        exit(1)

# Avro Schema Loading
def load_avro_schema(schema_path='producer/schemas/stock_price.avsc'):
    """Loads an Avro schema from a file."""
    try:
        with open(schema_path, 'r') as file:
            return avro.loads(file.read())
    except FileNotFoundError:
        logging.error(f"Avro schema file not found at {schema_path}")
        exit(1)

# Data Fetching
def fetch_stock_data(symbols):
    """Fetches the latest stock data for a list of symbols from Yahoo Finance."""
    logging.info(f"Fetching data for symbols: {symbols}")
    try:
        data = yf.Tickers(symbols)
        return data.tickers
    except Exception as e:
        logging.error(f"Error fetching stock data: {e}")
        return {}

# Main Application Logic
def run_producer():
    """Initializes and runs the Kafka producer."""
    config = load_config()
    key_schema = load_avro_schema('producer/schemas/stock_price_key.avsc') 
    value_schema = load_avro_schema('producer/schemas/stock_price.avsc')

    producer_config = {
        'bootstrap.servers': config['kafka']['bootstrap_servers'],
        'schema.registry.url': config['kafka']['schema_registry_url']
    }

    producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)
    symbols = config['stocks']
    topic = config['kafka']['topic_name']

    while True:
        tickers = fetch_stock_data(symbols)
        if not tickers:
            logging.warning("No data fetched, waiting before retry.")
            time.sleep(10)
            continue

        for symbol, ticker_info in tickers.items():
            try:
                info = ticker_info.info
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

                producer.produce(topic=topic, key=key, value=message)
                logging.info(f"Produced message for {symbol}: {message}")

            except Exception as e:
                logging.error(f"Failed to process or produce message for {symbol}: {e}")
        
        producer.flush()
        logging.info("Batch sent, waiting for 10 seconds")
        time.sleep(10)

if __name__ == "__main__":
    import os
    if os.path.exists('config/stocks.yaml'):
        run_producer()
    else:
        os.chdir('../..')
        run_producer()
