
import csv
import json
from producer import publish_event

#process of the code:
#csv file gets read row by row-> each row gets sent as a json to the pipeline
#changes to be made: send multiple rows at once???

def process_csv_row_by_row(csv_file, kafka_topic):
    """
    Read CSV file row by row, convert each row to JSON, and send it to Kafka.
    """
    print(f"Sending records to Kafka topic: {kafka_topic}")

    # Open the CSV file and process it row by row
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')  # Modify delimiter if necessary
        # Iterate over each row
        for row in reader:
            try:
                # Convert the row (dict) to JSON and publish it to Kafka
                publish_event(event=row, topic=kafka_topic)
                print(f"Record sent to topic {kafka_topic}: {row}")

            except Exception as e:
                print(f"Error sending record to Kafka: {e}")
                # Optionally log or handle errors for individual rows

if __name__ == "__main__":
    # Define the CSV file path and Kafka topic
    csv_file_path = './redfin_weekly_data.csv'  # Replace with the path to your CSV file
    kafka_topic = 'Aryan_dhanuka123'  # Replace with your Kafka topic name

    # Process the CSV file row by row and send each record to Kafka
    process_csv_row_by_row(csv_file_path, kafka_topic)

