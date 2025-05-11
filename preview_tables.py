import pandas as pd
import os
import glob

data_dir = 'data'
tables = [name for name in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, name)) and not name.startswith('.') and not name.startswith('_')]

for table in tables:
    print(f'\n=== {table} ===')
    parquet_files = glob.glob(os.path.join(data_dir, table, '*.parquet'))
    if not parquet_files:
        print('No parquet files found.')
        continue
    for pq_file in parquet_files:
        print(f'File: {pq_file}')
        df = pd.read_parquet(pq_file)
        print(df)
