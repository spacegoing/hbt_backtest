import pandas as pd
import dask.dataframe as dd
from dask import delayed
import time

# csv_file = './orderbook/csv/BTCUSDT_T_DEPTH_2022-10-31_depth_snap.csv'
# csv_file_list = [
#     './orderbook/csv/BTCUSDT_T_DEPTH_2022-11-01_depth_update.csv',
#     './orderbook/csv/BTCUSDT_T_DEPTH_2022-11-02_depth_update.csv',
#     './orderbook/csv/BTCUSDT_T_DEPTH_2022-11-03_depth_update.csv'
# ]
# 1031 - 1103: 154379534, 136394246, 158424283, 131870002

# for csv_file in csv_file_list:
#   ddf = dd.read_csv(csv_file)
#   print(ddf.shape[0].compute())

#*

# Read the CSV files into Dask DataFrames
time1 = time.time()
snap_df = dd.read_csv(
    './orderbook/csv/BTCUSDT_T_DEPTH_2022-10-31_depth_snap.csv')
depth_df = dd.read_csv(
    './orderbook/csv/BTCUSDT_T_DEPTH_2022-10-31_depth_update.csv')

DEPTH_EVENT = 1
DEPTH_CLEAR_EVENT = 3
SNAPSHOT_EVENT = 4

# Combine the DataFrames
time2 = time.time()
total_df = dd.concat([snap_df, depth_df], interleave_partitions=True)

# Sort the DataFrame by 'timestamp'
total_df = total_df.sort_values('timestamp')

# Find the first row where update_type is 'snap'
first_snap_index = total_df[total_df['update_type'] ==
                            'snap'].index.compute().min()

# Remove all rows above the first 'snap' row
total_df = total_df.loc[first_snap_index:]

# Initialize an empty list to store delayed objects
delayed_rows = []

# Loop through the partitions in total_df
old_snap_ts = None
for partition in total_df.to_delayed():
  partition = partition.compute()

  for _, row in partition.iterrows():
    event_data = {}
    event_data['exch_timestamp'] = row['timestamp']
    event_data['local_timestamp'] = row['timestamp'] + 10_000
    event_data['side'] = 1 if row['side'] == 'b' else -1
    event_data['price'] = row['price']
    event_data['qty'] = row['qty']

    if row['update_type'] == 'snap':
      if old_snap_ts is None:
        old_snap_ts = row['timestamp']
        event_data['event'] = SNAPSHOT_EVENT
      else:
        if row['timestamp'] > old_snap_ts:
          # Insert DEPTH_CLEAR_EVENT rows
          for side in [-1, 1]:
            clear_event_data = {
                'event': DEPTH_CLEAR_EVENT,
                'exch_timestamp': old_snap_ts,
                'local_timestamp': old_snap_ts + 10_000,
                'side': side,
                'price': event_data['price'],
                'qty': 0
            }
            delayed_rows.append(delayed(pd.DataFrame([clear_event_data])))

          old_snap_ts = row['timestamp']
        event_data['event'] = SNAPSHOT_EVENT
    else:  # row['update_type'] == 'set'
      event_data['event'] = DEPTH_EVENT

    # Append the current row's event data as a delayed object
    delayed_rows.append(delayed(pd.DataFrame([event_data])))

# Compute and concatenate the list of delayed rows to create result_df as a Dask DataFrame
result_df = dd.from_delayed(delayed_rows)
result_df = result_df.compute()
time3 = time.time()
result_df.to_csv('result_df.csv', index=False, single_file=True)
time4 = time.time()
