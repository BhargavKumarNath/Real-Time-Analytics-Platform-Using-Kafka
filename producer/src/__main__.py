import yaml
import time
import logging
import random
# import yfinance as yf
from alpha_vantage.timeseries import TimeSeries 
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def load_config(config_path='config/stocks.yaml'):
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {config_path}")
        exit(1)

def load_avro_schema(schema_path):
    try:
        with open(schema_path, 'r') as file:
            return file.read()
    except FileNotFoundError:
        logging.error(f"Avro schema file not found at {schema_path}")
        exit(1)

def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def run_producer():
    config = load_config()
    kafka_config = config['kafka']
    alpha_vantage_config = config['alpha_vantage'] 
    symbols = config['stocks']
    topic = kafka_config['topic_name']
    
    # Initialize Alpha Vantage TimeSeries client
    ts = TimeSeries(key=alpha_vantage_config['api_key'], output_format='json')

    schema_registry_conf = {'url': kafka_config['schema_registry_url']}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    key_schema_str = load_avro_schema('producer/schemas/stock_price_key.avsc')
    value_schema_str = load_avro_schema('producer/schemas/stock_price.avsc')

    key_serializer = AvroSerializer(schema_registry_client, key_schema_str)
    value_serializer = AvroSerializer(schema_registry_client, value_schema_str)

    producer_conf = {'bootstrap.servers': kafka_config['bootstrap_servers']}
    producer = Producer(producer_conf)

    while True:
        logging.info("Starting new cycle of stock data fetching...")
        for symbol in symbols:
            try:
                logging.info(f"Fetching data for {symbol}...")
                # Fetch data using Alpha Vantage API
                data, meta_data = ts.get_quote_endpoint(symbol=symbol)

                if not data:
                    logging.warning(f"No data returned for {symbol}. Skipping.")
                    continue

                # Extract the relevant fields from the API response
                current_price = data.get('05. price')
                volume = data.get('06. volume')

                if current_price is None or volume is None:
                    logging.warning(f"Price or volume not available for {symbol}. Skipping.")
                    continue

                value_message = {
                    'timestamp': int(time.time() * 1000),
                    'symbol': symbol,
                    'price': float(current_price),
                    'volume': int(volume)
                }
                key_message = {'symbol': symbol}

                producer.produce(
                    topic=topic,
                    key=key_serializer(key_message, SerializationContext(topic, MessageField.KEY)),
                    value=value_serializer(value_message, SerializationContext(topic, MessageField.VALUE)),
                    on_delivery=delivery_report
                )
                logging.info(f"Successfully produced message for {symbol}")

            except Exception as e:
                logging.error(f"Failed to fetch or process data for {symbol}: {e}")

            finally:
                sleep_duration = 15
                logging.info(f"Waiting for {sleep_duration} seconds to respect API rate limits...")
                time.sleep(sleep_duration)
        
        # Poll and flush after each full cycle
        producer.poll(1)
        producer.flush()
        logging.info("--- Full cycle completed. Starting a new one shortly. ---")
        time.sleep(60) # Longer wait after a full cycle



if __name__ == "__main__":
    config_path = 'config/stocks.yaml'
    if not os.path.exists(config_path):
        os.chdir('../..')
    
    if os.path.exists(config_path):
        run_producer()
    else:
        logging.error("Could not find config/stocks.yaml from expected paths.")
