
from kafka import KafkaConsumer
import json
import threading

def start_kafka_consumer(topic, process_callback, bootstrap_servers='localhost:9092'):
    """
    Start a Kafka consumer that listens to a specified topic, batches messages for up to 20 seconds,
    and processes the batch with a callback function.

    :param topic: The Kafka topic to listen to.
    :param process_callback: The callback function to process each message batch.
    :param bootstrap_servers: Kafka server address (default is 'localhost:9092').
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )

    print(f"Listening to Kafka topic '{topic}'...")

    # Variables to manage batching
    batch = []  # Buffer to accumulate events
    batch_lock = threading.RLock()  # Use RLock instead of Lock
    batch_timer = None  # Timer for managing 20-second batching window

    def flush_batch():
        """Flush the current batch by sending it to the callback and clearing the buffer."""
        nonlocal batch, batch_timer
        with batch_lock:
            if batch:
                # Copy the batch to avoid modification during processing
                current_batch = batch.copy()
                batch.clear()  # Clear the batch after copying
                batch_timer = None  # Reset the timer
            else:
                return  # No events to flush

        # Process the batch outside the lock to avoid blocking other operations
        print(f"Flushing batch of {len(current_batch)} events to callback.")
        process_callback(current_batch)

    def start_batch_timer():
        """Start the 20-second timer to trigger batch flush."""
        nonlocal batch_timer
        with batch_lock:
            if batch_timer is None:
                batch_timer = threading.Timer(20, flush_batch)
                batch_timer.start()
                print("Started a new 20-second batch timer.")

    for message in consumer:
        event = message.value  # Extract the event data
        print(f"Received event: {event}")

        # Add the event to the batch
        with batch_lock:
            batch.append(event)
            print(f"Event added to batch. Current batch size: {len(batch)}")

            # Start the timer if this is the first event in the batch
            if len(batch) == 1:
                start_batch_timer()

    # Ensure any remaining messages are flushed when the consumer stops
    flush_batch()





