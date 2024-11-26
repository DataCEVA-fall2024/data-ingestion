
from kafka import KafkaConsumer
import json
import threading

def start_kafka_consumer(topics, process_callback, bootstrap_servers='localhost:9092'):
    """
    Start a Kafka consumer that listens to a specified topic, batches messages for up to 20 seconds,
    and processes the batch with a callback function.

    :param topic: The Kafka topic to listen to.
    :param process_callback: The callback function to process each message batch.
    :param bootstrap_servers: Kafka server address (default is 'localhost:9092').
    """
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )

    print(f"Listening to Kafka topic '{topics}'...")

    # Variables to manage batching
    batches = {} 
    batch_locks = {}
    batch_timers = {}

    for topic in topics:
        batches[topic] = []
        batch_lock[topic] = threading.RLock()
        batch_timers[topic] = None

    def flush_batch(topic):
        """Flush the current batch by sending it to the callback and clearing the buffer."""
        with batch_locks[topic]:
            current_batch = batches[topic].copy()
            batches[topic].clear()
            batch_timers[topic] = None

        if current_batch:
            print(f"Flushing batch of {len(current_batch)} events to callback.")
            process_callback(current_batch, topic=topic)

    def start_batch_timer(topic):
        """Start the 20-second timer to trigger batch flush."""
        with batch_locks[topic]:
            if batch_timers[topic] is None:
                batch_timers[topic] = threading.Timer(20, flush_batch, args=(topic,))
                batch_timers[topic].start()
                print(f"Started a new 20-second batch timer for topic '{topic}'.")

    for message in consumer:
        event = message.value  # Extract the event data
        topic = message.topic
        print(f"Received event from topic '{topic}': {event}")

        # Add the event to the batch
        with batch_locks[topic]:
            batches[topic].append(event)
            print(f"Event added to batch for topic '{topic}'. Current batch size: {len(batches[topic])}")

            # Start the timer if this is the first event in the batch
            if len(batches[topic]) == 1:
                start_batch_timer(topic)

    # Ensure any remaining messages are flushed when the consumer stops
    for topic in topics:
        flush_batch(topic)





