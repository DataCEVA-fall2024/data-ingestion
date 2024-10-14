import requests
import pandas as pd

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

if __name__ == "__main__":
    csv_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_covid19/weekly_housing_market_data_most_recent.tsv000'
    local_filename = 'redfin_weekly_data.csv'

    download_large_file(csv_url, local_filename)
    process_large_csv(local_filename)
