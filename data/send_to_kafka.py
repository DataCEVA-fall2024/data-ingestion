
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

def main():
    parser = argparse.ArgumentParser(description="Kafka producer Script")
    parser.add_argument('-n', '--neighborhood', action='store_true', dest='neighborhood', help='If set run script to ingest neighborhood data')
    parser.add_argument('-w', '--weekly', action='store_true', dest='weekly', help='If set run script to ingest weekly data')

    args = parser.parse_args()
    if args.neighborhood:
        csv_file_path = './redfin_neighborhood_csv.csv'
        kafka_topic = 'neighborhood_data'
        process_csv_row_by_row(csv_file_path, kafka_topic)
    if args.weekly: 
        csv_file_path = './redfin_weekly_data.csv'  # Replace with the path to your CSV file
        kafka_topic = 'Aryan_dhanuka123'  # Replace with your Kafka topic name
        # Process the CSV file row by row and send each record to Kafka
        process_csv_row_by_row(csv_file_path, kafka_topic)
    else:
        print("No option selected, exiting script as no selection made for what to send to Kafka")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()

