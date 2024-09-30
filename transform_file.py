import pandas as pd
import gzip
import shutil
import ndjson
import json
import os

folder_path = 'your_folder_path'

# Define the input file and chunk size
input_file = 'your_file_path'
chunksize = 1024

# Check if the folder exists
if not os.path.exists(folder_path):
    # Create the folder
    os.makedirs(folder_path)

# Read the NDJSON file
with open(input_file) as f:
    data = ndjson.load(f)

# Convert to JSON
json_data = json.dumps(data)

# Read the file in chunks
chunk_iter = pd.read_json(json_data, lines=True, chunksize=chunksize)

# Process each chunk
for i, chunk in enumerate(chunk_iter):
    # Define the CSV file name for the chunk
    csv_file = f'your_folder_path/csv/chunk_{i}.csv'

    # Save the chunk to a CSV file
    chunk.to_csv(csv_file, index=False)
    
    # Compress the CSV file using GZIP
    with open(csv_file, 'rb') as f_in:
        with gzip.open(f'your_folder_path/gzip/{csv_file}.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
