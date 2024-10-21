from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    #value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
#Publish event
#event: the data in json format that represents the data being sent to the consumer
#topic: a string representing the endpoint relating to which the data is being sent. 
#can potentially be changed to an enum in order to restore control on the topics supported by the pipeline
def publish_event(event, topic):
    producer.send(topic, value=event)
    producer.flush()  # Ensures data is sent immediately

