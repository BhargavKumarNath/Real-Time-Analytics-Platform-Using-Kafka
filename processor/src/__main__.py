import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, TimestampType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

KAFKA_TOPIC_NAME = "stock_prices"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

def load_avro_schema(schema_path):
    """Loads an Avro schema from a file."""
    try:
        with open(schema_path, 'r') as file:
            return file.read()
    except FileNotFoundError:
        logging.error(f"Avro schema file not found at {schema_path}")
        exit(1)

def main():
    logging.info("Starting Spark Structured Streaming application...")

    # Load Avro schemas
    key_schema_str = load_avro_schema("schemas/stock_price_key.avsc")
    value_schema_str = load_avro_schema("schemas/stock_price.avsc")

    spark = SparkSession.builder \
        .appName("RealTimeStockTracker") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logging.info("SparkSession created successfully.")

    # 1. Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()
    
    logging.info("Kafka source stream initialized.")

    # 2. Deserialize Avro messages
    deserialized_df = kafka_df.select(
        from_avro(
            expr("substring(key, 6)"),  # Skip 5-byte header
            key_schema_str
        ).alias("key"),
        from_avro(
            expr("substring(value, 6)"), # Skip 5-byte header
            value_schema_str
        ).alias("value")
    )

    # 3. Flatten the structure
    stock_df = deserialized_df.select("value.*")
    
    logging.info("Avro deserialization and data flattening complete.")

    query = stock_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    logging.info("Console sink started. Awaiting termination...")
    query.awaitTermination()
    logging.info("Application terminated.")


if __name__ == "__main__":
    main()
