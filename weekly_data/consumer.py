from kafka import KafkaConsumer
import json
#Eventually will create multiple different topics each representing a different API endpoint to process the data accordingly.


# topic: the topic that we're creating this listener is being created for
# callback: the callback function that is supposed to be called with the data of the event. 
#usage: call this function from a python file with a definition of a callback function in order to process the data. 
def create_consumer(topic, callback):
    consumer=KafkaConsumer(
            topic, 
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest'
    )
    for message in consumer:
        event_data=message.value
        callback(event_data)

