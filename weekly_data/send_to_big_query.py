from google.cloud import bigquery
from google.cloud import bigquery_storage
from io import BytesIO
import fastavro
from consumer import consume_avro_from_kafka
from google.cloud.bigquery import LoadJobConfig, SourceFormat

# Initialize BigQuery client
client = bigquery.Client()

def send_to_bigquery_in_avro(records, schema, dataset_id, table_id):
    """
    Sends deserialized Avro records to BigQuery in Avro format.

    :param records: Deserialized Avro records.
    :param schema: Avro schema used for serialization.
    :param dataset_id: BigQuery dataset ID.
    :param table_id: BigQuery table ID.
    """
    try:
        # Create a BytesIO buffer to store the Avro data
        avro_bytes_io = BytesIO()

        # Write records to Avro format using fastavro
        fastavro.writer(avro_bytes_io, schema, records)

        # Reset buffer position to the start
        avro_bytes_io.seek(0)

        # BigQuery table reference
        table_ref = client.dataset(dataset_id).table(table_id)

        # Configure the load job to accept Avro format
        job_config = LoadJobConfig()
        job_config.source_format = SourceFormat.AVRO

        # Load the Avro data into BigQuery
        load_job = client.load_table_from_file(avro_bytes_io, table_ref, job_config=job_config)

        # Wait for the job to complete
        load_job.result()

        print(f"Loaded {load_job.output_rows} rows into {dataset_id}.{table_id}")

    except Exception as e:
        print(f"Error while sending to BigQuery: {e}")

def callbackfunc(data):
    process_records_for_bigquery(
    )
    
def process_records_for_bigquery(records, schema, dataset_id, table_id):
    """
    Callback function to process Avro records and send them to BigQuery.

    :param records: Deserialized Avro records.
    :param schema: Avro schema for the records.
    :param dataset_id: BigQuery dataset ID.
    :param table_id: BigQuery table ID.
    """
    print(f"Processing {len(records)} records for BigQuery.")
    send_to_bigquery_in_avro(records, schema, dataset_id, table_id)

if __name__ == "__main__":
    kafka_topic = 'your_topic_name'
    kafka_broker = 'localhost:9092'
    schema_file_path = '/mnt/c/Users/Aryan/Projects/sem7/CS639/data-ingestion/weekly_data/weekly_data_schema.avsc'
    dataset_id = 'your_dataset'
    table_id = 'your_table'

    # Use the consumer with the callback to send data to BigQuery
    consume_avro_from_kafka(
        kafka_topic=kafka_topic,
        kafka_broker=kafka_broker,
        schema_file=schema_file_path,
        callback=process_records_for_bigquery,
        dataset_id=dataset_id,
        table_id=table_id
    )
