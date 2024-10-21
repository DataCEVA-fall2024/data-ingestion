import fastavro
from kafka import KafkaConsumer
from io import BytesIO

def load_avro_schema(schema_file):
    """Load Avro schema from a file."""
    schema = fastavro.schema.load_schema(schema_file)
    return schema

def deserialize_avro(data, schema):
    """Deserialize Avro-encoded bytes using the provided schema."""
    bytes_io = BytesIO(data)
    reader = fastavro.reader(bytes_io, schema)
    records = [record for record in reader]
    return records

def consume_avro_from_kafka(kafka_topic, kafka_broker, schema_file,callback):
    """Consume Avro data from Kafka and deserialize it."""
    # Load Avro schema
    avro_schema = load_avro_schema(schema_file)

    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_broker,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: x  # Leave data as bytes
    )

    print(f"Listening for messages on topic: {kafka_topic}")

    # Consume messages
    for message in consumer:
        avro_data = message.value  # This will be the Avro-encoded byte data
        try:
            # Deserialize Avro bytes
            records = deserialize_avro(avro_data, avro_schema)
            callback(records)
        except Exception as e:
            print(f"Failed to deserialize Avro data: {e}")

if __name__ == "__main__":
    kafka_topic = 'your_topic_name'  # Kafka topic to consume from
    kafka_broker = 'localhost:9092'  # Kafka broker
    schema_file_path = '/mnt/c/Users/Aryan/Projects/sem7/CS639/data-ingestion/weekly_data/weekly_data_schema.avsc'  # Avro schema path

    consume_avro_from_kafka(kafka_topic, kafka_broker, schema_file_path)

