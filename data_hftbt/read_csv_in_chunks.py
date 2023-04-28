#* Python+numpy
import numpy as np
import csv

csv_file = 'large_dataset.csv'
chunk_size = 1000


def read_chunks(file_path, chunk_size):
  with open(file_path, 'r') as f:
    # Read the header (column names) and store the number of columns
    header = next(csv.reader(f))
    num_columns = len(header)

    # Read the file in chunks
    chunk_data = []
    for row in csv.reader(f):
      chunk_data.append(list(map(float, row)))
      if len(chunk_data) >= chunk_size:
        yield np.array(chunk_data)
        chunk_data = []

    # Yield the last chunk if there's any data left
    if chunk_data:
      yield np.array(chunk_data)


for chunk in read_chunks(csv_file, chunk_size):
  # Perform some operations with the NumPy array
  # ...
  print(chunk)

#* Pandas
import pandas as pd

csv_file = 'large_dataset.csv'
chunk_size = 1000

# Read the file in chunks
for chunk_df in pd.read_csv(csv_file, chunksize=chunk_size):
  # Perform some operations with the pandas DataFrame
  # ...
  print(chunk_df.head())


#*
import dask.dataframe as dd

csv_file = 'large_dataset.csv'

# Read the CSV file into a Dask DataFrame
ddf = dd.read_csv(csv_file)

# Perform some operations with the Dask DataFrame
# For example, let's compute the mean of a column named 'column_name'
mean_value = ddf['column_name'].mean().compute()
print(mean_value)

# If you need to convert a small portion of the Dask DataFrame to a pandas DataFrame,
# you can use the 'compute()' method, but be cautious as it may cause an OOM error if the data is too large
pdf = ddf.compute()

# To save the processed Dask DataFrame to a new CSV file
ddf.to_csv('output_*.csv', index=False)
