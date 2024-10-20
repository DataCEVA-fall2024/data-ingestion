import requests
import pandas as pd
import fastavro
import json


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

def convert_csv_to_avro(csv_filename, avro_filename, avro_schema):
    """
    Convert CSV file to an Avro file

    :param csv_filename: The path to the CSV file.
    :type csv_filename: str
    :param avro_filename: The path to save the Avro file.
    :type avro_filename: str
    :param avro_schema: The Avro schema for the file.
    :type avro_schema: dict 
    """
    try:
        df = pd.read_csv(csv_filename, sep='\t', low_memory=False)

        records = df.to_dict(orient='records')

        with open(avro_filename, 'wb') as avro_file:
            fastavro.writer(avro_file, avro_schema, records)

    except Exception as e:
        print(f"Error occured during CSV to Avro conversion: {e}")

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

if __name__ == "__main__":
    csv_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_covid19/weekly_housing_market_data_most_recent.tsv000'
    local_filename = 'redfin_weekly_data.csv'
    avro_filename = 'redfin_weekly_data.avro'
    schema_filename = 'weekly_data_schema.avsc'

    avro_schema = load_schema_from_file(schema_filename)

    download_large_file(csv_url, local_filename)
    #process_large_csv(local_filename)
    #read_first_n_records(avro_filename, 50)
    if avro_schema:
        convert_csv_to_avro(local_filename, avro_filename, avro_schema)
