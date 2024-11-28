import requests
import pandas as pd
import fastavro
from fastavro import writer, parse_schema
import json
import gzip
import os
import sys
import argparse
import csv


def avro_to_pd_dtypes(avro_schema):
    """ 
    Converts an Avro schema to a pandas dtype mapping.

    :param avro_schema: The Avro schema as a dictionary
    :type avro_schema: dict
    :return: Dictionary mapping field names to pd tyes
    :rtype: dict
    """

    avro_type_to_pd = {
        'string': 'str',
        'int': 'Int32',  # Use pandas nullable integer type
        'long': 'Int64',  # Use pandas nullable integer type
        'float': 'float32',
        'double': 'float64',
        'boolean': 'bool',
        'bytes': 'bytes',
        'date': 'str',  # Dates can be parsed later
        'timestamp-micros': 'str',  # Timestamps can be parsed later
    }

    dtype_map = {}

    for field in avro_schema.get('fields', []):
        field_name = field['name']
        field_type = field['type']

        if isinstance(field_type, list):
            types = [t for t in field_type if t != 'null']
            if len(types) = 1:
                field_type = types[0]
            else:
                field_type = 'string'
        if isinstance(field_type, dict):
            logical_type = field_type.get('logicalType')
            if logical_type:
                if logical_type == 'date':
                    dtype = 'str'  # Or 'object' if you plan to parse dates later
                elif logical_type == 'timestamp-micros':
                    dtype = 'str'  # Or 'object' for parsing
                else:
                    dtype = 'str'
            else:
                # Handle other complex types if necessary
                dtype = 'str'
        else:
            dtype = avro_type_to_pd.get(field_type, 'str')

        dtype_map[field_name] = dtype

    return dtype_map



def unzip_large_file(input_file, output_file):
    """
    Unzips a large file in chunks to not overload memory usage

    :param input_file: The name of file to unzip
    :type input_file: str
    :param output_file: The name of file to output unzip content to
    :type output_file: str
    """
    buffer_size = 500 * 1024 * 1024

    file_size = os.path.getsize(input_file)
    processed_size = 0

    try:
        with gzip.open(input_file, 'rb') as f_in:
            with open(output_file, 'wb') as f_out:
                while True:
                    chunk = f_in.read(buffer_size)
                    if not chunk:
                        break
                    f_out.write(chunk)
                    processed_size += len(chunk)
                    progress = (processed_size / file_size) * 100
                    sys.stdout.write(f'\rProgress: {progress:.2f}%')
                    sys.stdout.flush()
        print('\nUnzip succesfully completed')
    except MemoryError:
        print('MemoryError error, buffer size too large')
    except Exception as e:
        print(f'Exception occured: {e}')


def download_large_file(url, filename):
    """
    Downloads a large file from the given URL in streaming mode.

    :param url: The URL of the file to download.
    :type url: str
    :param filename: The local filename to save the file.
    :type filename: str
    """
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            with open(filename, 'wb') as file:
                for data in response.iter_content(chunk_size=1024 * 1024):
                    file.write(data)
        print(f"Download completed. Saved as {filename}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")

def process_large_csv(filename):
    """
    Reads and processes a large CSV file in chunks.

    :param filename: The path to the CSV file.
    :type filename: str
    """
    chunksize = 10 ** 6  # Adjust the chunk size as needed
    total_rows_processed = 0

    # Define the data types for each column if known to optimize memory usage
    # Example: dtype = {'column1': str, 'column2': float, ...}
    dtype = None  # Replace with actual data types if known

    # Read the TSV file in chunks
    reader = pd.read_csv(
        filename,
        sep='\t',            # Tab-separated file
        chunksize=chunksize,
        dtype=dtype,
        low_memory=False,    # Improve performance
        engine='c',          # Use the C engine for parsing
        na_values=['', ' '], # Handle missing values
    )

    for i, chunk in enumerate(reader):
        # Example processing: print the first few rows of each chunk
        print(f"Processing Chunk {i + 1}:")
        print(chunk.head())

        # Perform any data manipulation or analysis here
        # For example, calculate summary statistics
        # summary = chunk.describe()

        # Keep track of the total number of rows processed
        total_rows_processed += len(chunk)

    print(f"Total rows processed: {total_rows_processed}")

def convert_csv_to_avro(csv_filename, avro_filename, avro_schema, chunksize=10**6):
    """
    Convert CSV file to an Avro file

    :param csv_filename: The path to the CSV file.
    :type csv_filename: str
    :param avro_filename: The path to save the Avro file.
    :type avro_filename: str
    :param avro_schema: The Avro schema for the file.
    :type avro_schema: dict 
    """

    dtype = avro_to_pd_dtypes(avro_schema)
    na_val = ['', ' ', 'NA', 'NaN', None]
    def record_generator():
        try:
            print("Reading CSV in chunks")
            for chunk in pd.read_csv(
                    csv_filename, 
                    sep='\t', 
                    chunksize=chunksize, 
                    dtype=dtype,
                    na_values=na_val,
                    keep_deafult_na=True,
                    low_memory=True):
                print("Processing chunk")
                chunk = chunk.where(pd.notnull(chunk), None)
                for record in chunk.itertuples(index=False, name=None):
                    yield dict(zip(chunk.columns, record))
        except Exception as e:
            print(f"Error reading CSV in chunks: {e}")
            raise

    parsed_schema = parse_schema(avro_schema)

    try:
        with open(avro_filename, 'wb') as out:
            writer(out, parsed_schema, record_generator())
        print(f"CSV successfully converted to AVRO: {avro_filename}")
    except Exception as e:
        print(f"Error occurred during CSV to Avro conversion: {e}")   

def read_first_n_records(avro_file, n):
    """
    Reads the first n records from an Avro file and prints them.

    :param avro_file: The path to the Avro file.
    :type avro_file: str
    :param n: The number of records to read.
    :type n: int
    """
    try:
        with open(avro_file, 'rb') as f:
            reader = fastavro.reader(f)
            for i, record in enumerate(reader):
                if i >= n:
                    break
                print(record)
    except Exception as e:
        print(f"Error reading Avro file: {e}")

def load_schema_from_file(schema_path):

    try:
        with open(schema_path, 'r') as schema_file:
            schema = json.load(schema_file)
        return schema
    except Exception as e:
        print(f"Error loading schema from file: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="data ingestion script")
    parser.add_argument('-n', '--neighborhood', action='store_true', dest='neighborhood', help='If set run script to ingest neighborhood data')
    parser.add_argument('-w', '--weekly', action='store_true', dest='weekly', help='If set run script to ingest weekly data')

    args = parser.parse_args()

    if args.neighborhood:
        print("Getting neighborhood data")
        neighbor_zip_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/neighborhood_market_tracker.tsv000.gz'
        neighborhood_zip_filename = 'redfin_neighborhood_zip.gz'
        neighborhood_csv_filename = 'redfin_neighborhood_csv.csv'
        avro_neighbor_schema_filename = 'neighborhood_data_schema.avsc'
        avro_neighbor_data_filename = 'redfin_neighborhood_data.avro'

        avro_neighbor_schema = load_schema_from_file(avro_neighbor_schema_filename)

        print("Starting download of the CSV file...")
        download_large_file(neighbor_zip_url, neighborhood_zip_filename)
        unzip_large_file(neighborhood_zip_filename, neighborhood_csv_filename)
        if avro_neighbor_schema:
            print("Starting CSV to Avro conversion...")
            convert_csv_to_avro(neighborhood_csv_filename, avro_neighbor_data_filename, avro_neighbor_schema)

        print("Completed process for getting weekly data")

    if args.weekly: 
        weekly_csv_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_covid19/weekly_housing_market_data_most_recent.tsv000'
        weekly_data_filename = 'redfin_weekly_data.csv'
        weekly_avro_filename = 'redfin_weekly_data.avro'
        weekly_schema_filename = 'weekly_data_schema.avsc'

        avro_schema = load_schema_from_file(weekly_schema_filename)

        print("Starting download of the CSV file...")
        download_large_file(weekly_csv_url, weekly_data_filename)

        if avro_schema:
            print("Starting CSV to Avro conversion...")
            convert_csv_to_avro(local_filename, avro_filename, avro_schema)
        print("Completed process for getting weekly data")
    else:
        print("No option selected, exiting script as no selection made for what to ingest")
        sys.exit(1)


    sys.exit(0)

if __name__ == "__main__":
    main()
