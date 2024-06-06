import pandas as pd
import numpy as np

def split_csv(input_file, num_records_per_chunk):
    df = pd.read_csv(input_file)
    num_rows = len(df)
    num_chunks = -(-num_rows // num_records_per_chunk)  # Ceiling division to get the number of chunks

    for i, chunk in enumerate(np.array_split(df, num_chunks)):
        chunk.to_csv(f"new_chunk_{i + 1}.csv", index=False)

# Replace 'input_file.csv' with your CSV file and specify the number of records per chunk
split_csv('file.csv', 5)  # Change '5' to the desired number of records per file
