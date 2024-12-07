import fastavro
from consumer import start_kafka_consumer
from io import BytesIO
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, SourceFormat
import json
from datetime import datetime
import os
import threading
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ='/mnt/c/Users/Aryan/Downloads/uw-capstone-b295368271ff.JSON'
# Initialize BigQuery client
client = bigquery.Client()


CONFIG = {
    'Aryan_dhanuka123': {
        'schema_file_path': './weekly_data_schema.avsc',
        'table_id': 'raw_county_data'
    },
    'neighborhood_data': {
        'schema_file_path': './neighborhood_data_schema.avsc',
        'table_id': 'raw_neighborhood_data'
    },
}
dataset_id = 'raw_zone'

topic_names = list(CONFIG.keys())

def load_avro_schema(schema_file):
    """Load Avro schema from a file."""
    return fastavro.schema.load_schema(schema_file)

def validate_and_convert_types(json_record, avro_schema):
    """Validate and convert the JSON record's fields based on the Avro schema."""
    converted_record = {}

    for field in avro_schema['fields']:
        field_name = field['name']
        field_type = field['type']

        if isinstance(field_type, list):
            field_type = [t for t in field_type if t != 'null'][0]

        value = json_record.get(field_name)
        if value in [None, '', 'null']:
            converted_record[field_name] = None
        else:
            try:
                if isinstance(field_type, dict) and 'logicalType' in field_type:
                    logical_type = field_type['logicalType']
                    if logical_type == 'date':
                        converted_record[field_name] = datetime.strptime(value, '%Y-%m-%d').date()
                    elif logical_type == 'timestamp-micros':
                        converted_record[field_name] = datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
                    else:
                        converted_record[field_name] = value
                elif field_type == 'int':
                    converted_record[field_name] = int(float(value))
                elif field_type == 'long':
                    converted_record[field_name] = int(float(value))
                elif field_type == 'float':
                    converted_record[field_name] = float(value)
                elif field_type == 'double':
                    converted_record[field_name] = float(value)
                elif field_type == 'boolean':
                    converted_record[field_name] = value.lower() == 'true' if isinstance(value, str) else bool(value)
                elif field_type == 'string':
                    converted_record[field_name] = str(value)
                else:
                    converted_record[field_name] = value
            except (ValueError, TypeError) as e:
                print(f"Type conversion error for field '{field_name}' with value '{value}': {e}")
                converted_record[field_name] = None
    return converted_record

def convert_batch_to_avro(batch, avro_schema):
    """Convert a batch of JSON records to Avro format with data type validation and conversion."""

    avro_bytes_io = BytesIO()
    validated_records = []
    invalid_records = []

    for record in batch:
        validated_record = validate_and_convert_types(record, avro_schema)
        if fastavro.validation.validate(validated_record, avro_schema):
            validated_records.append(validated_record)
        else:
            invalid_records.append(record)

    if invalid_records:
        print(f"Found {len(invalid_records)} invalid records.")

    if not validated_records:
        print("No valid records to write.")
        return None

    try:
        fastavro.writer(avro_bytes_io, avro_schema, validated_records)
    except Exception as e:
        print(f"Error during Avro batch conversion: {e}")
        return None

    avro_bytes_io.seek(0)
    return avro_bytes_io.getvalue()
def send_to_bigquery_in_avro(avro_data, dataset_id, table_id):
    """Send Avro data to BigQuery."""
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = LoadJobConfig()
    job_config.source_format = SourceFormat.AVRO

    load_job = client.load_table_from_file(BytesIO(avro_data), table_ref, job_config=job_config)
    load_job.result()
    print(f"Loaded {load_job.output_rows} rows into {dataset_id}.{table_id}")

def process_batch_and_upload_to_bigquery(batch, topic, **kwargs):
    """Process a batch of JSON records, convert to Avro, and upload to BigQuery."""
    config = CONFIG.get(topic)
    if not config:
        print(f"No configuration found for topic: {topic}")
        return 
    
    avro_schema = load_avro_schema(config['schema_file_path'])
    avro_data = convert_batch_to_avro(batch, avro_schema)
    if avro_data:
        send_to_bigquery_in_avro(avro_data, dataset_id, config['table_id'])


if __name__ == "__main__":
    start_kafka_consumer(
        topic=topic_names,
        process_callback=process_batch_and_upload_to_bigquery
    )
