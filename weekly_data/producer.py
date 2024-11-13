import json
from kafka import KafkaProducer

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize the event data to JSON
)

def publish_event(event, topic):
    """Publish an event to a Kafka topic."""
    try:
        # Send the event to Kafka
        future = producer.send(topic, value=event)
    except Exception as e:
        print(f"Error during Kafka send: {e}")




