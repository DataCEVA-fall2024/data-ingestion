from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, SourceFormat
from io import BytesIO
import fastavro
from consumer import consume_avro_from_kafka

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
        job_config.schema = [bigquery.SchemaField("period_begin", "DATE"),
                             bigquery.SchemaField("period_end", "DATE"),
                             bigquery.SchemaField("region_type", "STRING"),
                             bigquery.SchemaField("region_type_id", "INTEGER"),
                             bigquery.SchemaField("region_name", "STRING"),
                             bigquery.SchemaField("region_id", "INTEGER"),
                             bigquery.SchemaField("duration", "STRING"),
                             bigquery.SchemaField("adjusted_average_new_listings", "FLOAT"),
                             bigquery.SchemaField("adjusted_average_new_listings_yoy", "FLOAT"),
                             bigquery.SchemaField("average_pending_sales_listing_updates", "FLOAT"),
                             bigquery.SchemaField("average_pending_sales_listing_updates_yoy", "FLOAT"),
                             bigquery.SchemaField("off_market_in_two_weeks", "FLOAT"),
                             bigquery.SchemaField("off_market_in_two_weeks_yoy", "FLOAT"),
                             bigquery.SchemaField("adjusted_average_homes_sold", "FLOAT"),
                             bigquery.SchemaField("adjusted_average_homes_sold_yoy", "FLOAT"),
                             bigquery.SchemaField("median_new_listing_price", "FLOAT"),
                             bigquery.SchemaField("median_new_listing_price_yoy", "FLOAT"),
                             bigquery.SchemaField("median_sale_price", "FLOAT"),
                             bigquery.SchemaField("median_sale_price_yoy", "FLOAT"),
                             bigquery.SchemaField("median_days_to_close", "FLOAT"),
                             bigquery.SchemaField("median_days_to_close_yoy", "FLOAT"),
                             bigquery.SchemaField("median_new_listing_ppsf", "FLOAT"),
                             bigquery.SchemaField("median_new_listing_ppsf_yoy", "FLOAT"),
                             bigquery.SchemaField("active_listings", "FLOAT"),
                             bigquery.SchemaField("active_listings_yoy", "FLOAT"),
                             bigquery.SchemaField("median_days_on_market", "FLOAT"),
                             bigquery.SchemaField("median_days_on_market_yoy", "FLOAT"),
                             bigquery.SchemaField("percent_active_listings_with_price_drops", "FLOAT"),
                             bigquery.SchemaField("percent_active_listings_with_price_drops_yoy", "FLOAT"),
                             bigquery.SchemaField("age_of_inventory", "FLOAT"),
                             bigquery.SchemaField("age_of_inventory_yoy", "FLOAT"),
                             bigquery.SchemaField("months_of_supply", "FLOAT"),
                             bigquery.SchemaField("months_of_supply_yoy", "FLOAT"),
                             bigquery.SchemaField("median_pending_sqft", "FLOAT"),
                             bigquery.SchemaField("median_pending_sqft_yoy", "FLOAT"),
                             bigquery.SchemaField("average_sale_to_list_ratio", "FLOAT"),
                             bigquery.SchemaField("average_sale_to_list_ratio_yoy", "FLOAT"),
                             bigquery.SchemaField("median_sale_ppsf", "FLOAT"),
                             bigquery.SchemaField("median_sale_ppsf_yoy", "FLOAT")]

        # Load the Avro data into BigQuery
        load_job = client.load_table_from_file(avro_bytes_io, table_ref, job_config=job_config)

        # Wait for the job to complete
        load_job.result()

        print(f"Loaded {load_job.output_rows} rows into {dataset_id}.{table_id}")

    except Exception as e:
        print(f"Error while sending to BigQuery: {e}")
    
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
    dataset_id = 'uw-capstone.real_estate_data'
    table_id = 'uw-capstone.real_estate_data.redfin_data'

    # Use the consumer with the callback to send data to BigQuery
    consume_avro_from_kafka(kafka_topic, kafka_broker, schema_file_path,
        callback=lambda records: process_records_for_bigquery(records, schema_file_path, dataset_id, table_id))
