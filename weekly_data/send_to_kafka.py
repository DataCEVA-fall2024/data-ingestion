'''
import fastavro
import os

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from uuid import uuid4

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    :param err: The error that occured on none on success.
    :type: KafkaError
    :param msg: The message that was produced or failed
    :type: Message

    
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        need bound callback or lambda where you pass
        the objects along.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

def send_avro_to_kafka(avro_file, schema_registry_url, kafka_topic, kafka_broker, schema_file):
    """
    Send Avro data from an Avro file to Kafka.

    :param avro_file: Path to the Avro file.
    :param schema_registry_url: URL of the schema registry.
    :param kafka_topic: Kafka topic to send messages to.
    :param kafka_broker: Kafka broker URL.
    :param schema_file: Path to the Avro schema file.
    """
    # Load Avro schema

    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/avro/{schema_file}") as f:
        schema_str = f.read()

    schema_registry_config = {'url': schema_registry_url}
    schema_registry_client = SchemaRegistryClient(schema_registry_config)


    string_serializer = StringSerializer('utf-8')

    producer_conf = {'bootstrap.servers': kafka_broker }
    producer = Producer(producer_conf)

    print("Producing records to topic")

    # Initialize Kafka Avro Producer
    # Open and read the Avro file
    with open(avro_file, 'rb') as f:
        reader = fastavro.reader(f)
        for record in reader:
            try:
                # Send each record to Kafka
                producer.produce(topic=kafka_topic,
                                 key=string_serializer(str(uuid4())),
                                 value=record,
                                 on_delivery=delivery_report)
                producer.flush()
                print(f"Record sent to topic {kafka_topic}: {record}")
            except Exception as e:
                print(f"Failed to send record to Kafka: {e}")

if __name__ == "__main__":
    avro_file_path = 'redfin_weekly_data.avro'
    schema_file_path = 'weekly_data_schema.avsc' 
    kafka_topic = 'your_topic_name' # Kafka to topic that we want to send records to 
    kafka_broker = 'localhost:9092'  # Kafka broker URL when setup
    schema_registry_url = 'http://localhost:8081'  # Schema registry 

    send_avro_to_kafka(avro_file_path, schema_registry_url, kafka_topic, kafka_broker, schema_file_path)
'''
import fastavro
import os
from uuid import uuid4
from confluent_kafka import KafkaError

# Assuming this comes from the earlier pipeline code
from producer import publish_event

def delivery_report(err, msg):
    """Reports the success or failure of message delivery to Kafka."""
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
        return
    print(f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def send_avro_to_kafka(avro_file, kafka_topic, schema_file):
    """
    Send Avro data from an Avro file to Kafka using the publish_event function from the pipeline.
    
    :param avro_file: Path to the Avro file.
    :param kafka_topic: Kafka topic to send messages to.
    :param schema_file: Path to the Avro schema file.
    """
    # Load Avro schema
    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/avro/{schema_file}") as f:
        schema_str = f.read()

    print(f"Producing records to Kafka topic: {kafka_topic}")

    # Open and read the Avro file
    with open(avro_file, 'rb') as f:
        reader = fastavro.reader(f)
        for record in reader:
            try:
                # Use the publish_event function from the pipeline to send the event to Kafka
                publish_event(event=record, topic=kafka_topic)
                print(f"Record sent to topic {kafka_topic}: {record}")
            except KafkaError as e:
                print(f"Failed to send record to Kafka: {e}")

if __name__ == "__main__":
    # Define input files and Kafka settings
    avro_file_path = 'redfin_weekly_data.avro'
    schema_file_path = 'weekly_data_schema.avsc'
    kafka_topic = 'your_topic_name'  # Kafka topic to which records will be sent

    # Call the function to send Avro records to Kafka using the publish_event function
    send_avro_to_kafka(avro_file_path, kafka_topic, schema_file_path)

