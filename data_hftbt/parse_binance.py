import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask
import time
from hftbacktest.reader import TRADE_EVENT, DEPTH_EVENT, DEPTH_CLEAR_EVENT, DEPTH_SNAPSHOT_EVENT

date = '2022-10-31'

# Read the CSV files into Dask DataFrames
snap_df = dd.read_csv(f'./orderbook/csv/BTCUSDT_T_DEPTH_{date}_depth_snap.csv')
depth_df = dd.read_csv(
    f'./orderbook/csv/BTCUSDT_T_DEPTH_{date}_depth_update.csv')
trades_df = dd.read_csv(f'./trades/csv/BTCUSDT-trades-{date}.csv')

#* Preprocess DataFrames
depth_df['event_type'] = DEPTH_EVENT
depth_df['exch_time'] = depth_df['timestamp'] * 1000
depth_df['local_time'] = depth_df['exch_time'] + 50000
depth_df['side'] = depth_df['side'].map(lambda x: 1 if x == 'b' else -1)
depth_df = depth_df[[
    'event_type', 'exch_time', 'local_time', 'side', 'price', 'qty'
]]

trades_df['event_type'] = TRADE_EVENT
trades_df['exch_time'] = trades_df['time'] * 1000
trades_df['local_time'] = trades_df['exch_time'] + 50000
trades_df['side'] = (-2 * trades_df['is_buyer_maker'] + 1).astype('int8')
trades_df = trades_df[[
    'event_type', 'exch_time', 'local_time', 'side', 'price', 'qty'
]]

snap_df['event_type'] = DEPTH_SNAPSHOT_EVENT
snap_df['exch_time'] = snap_df['timestamp'] * 1000
snap_df['local_time'] = snap_df['exch_time'] + 50000
snap_df['side'] = snap_df['side'].map(lambda x: 1 if x == 'b' else -1)
snap_df = snap_df[[
    'event_type', 'exch_time', 'local_time', 'side', 'price', 'qty'
]]
grouped = snap_df.groupby('exch_time')


# Define a function that takes a group and returns the new rows to be inserted
def insert_depth_clear(group):
  # Filter the group to only include 'b' or 'a' orders and find the worst bid and ask prices
  bids = group.query('side == 1')
  worst_bid = bids['price'].min()
  worst_bid_qty = bids.query('price == @worst_bid')['qty'].iloc[0]
  asks = group.query('side == -1')
  worst_ask = asks['price'].max()
  worst_ask_qty = asks.query('price == @worst_ask')['qty'].iloc[0]
  # Create the new rows to be inserted
  new_rows = pd.DataFrame([{
      'event_type': DEPTH_CLEAR_EVENT,
      'exch_time': group['exch_time'].min(),
      'local_time': group['local_time'].min(),
      'side': 1,
      'price': worst_bid,
      'qty': worst_bid_qty
  }, {
      'event_type': DEPTH_CLEAR_EVENT,
      'exch_time': group['exch_time'].min(),
      'local_time': group['local_time'].min(),
      'side': -1,
      'price': worst_ask,
      'qty': worst_ask_qty
  }])
  # Concatenate the new rows with the original group and return the result
  res = pd.concat([new_rows, group], axis=0)
  return res


# Apply the insert_depth_clear function to each group and concatenate the results
snap_df = grouped.apply(
    insert_depth_clear, meta=snap_df.dtypes.to_dict()).reset_index(drop=True)

#* merging all 3 dfs
# Find the index of the first occurrence of the DEPTH_CLEAR_EVENT row
first_snap_ts = snap_df['exch_time'].loc[0].compute().item()
# Find the index of the first depth row whose 'exch_time' is later than first_snap_ts
depth_index = depth_df[
    depth_df['exch_time'] > first_snap_ts].index.min().compute()
depth_df = depth_df.loc[depth_index:]
# Find the index of the first trades row whose 'exch_time' is later than first_snap_ts
trades_index = trades_df[
    trades_df['exch_time'] > first_snap_ts].index.min().compute()
trades_df = trades_df[trades_index:]

# Save the data to disk as an npz file

# Get iterators for each dataframe
snap_it = snap_df.iterrows()
depth_it = depth_df.iterrows()
trades_it = trades_df.iterrows()

# Set the row size to the memory usage of a single row
row_size = snap_df.loc[0].memory_usage(index=False, deep=True).sum().compute()

# Set the chunk counter to 0
chunk_counter = 0

# Set the chunk size to 500 MB
chunk_size = 500 * 1024 * 1024

# Define the file name format
fn_format = f'BTCUSDT_{date}_{{chunk}}.npz'

# Initialize an empty list to hold the merged data
merged_list = []

# Get the first row for each dataframe
snap_row = next(snap_it, (None, None))[1]
depth_row = next(depth_it, (None, None))[1]
trades_row = next(trades_it, (None, None))[1]

# Merge the data and save it to disk in chunks
while snap_row is not None or depth_row is not None or trades_row is not None:
  # Select the row with the earliest exch_time
  rows = [row for row in [snap_row, depth_row, trades_row] if row is not None]
  selected_row = min(rows, key=lambda row: row['exch_time'])

  # Append the selected row to the merged list
  merged_list.append(selected_row.to_numpy())

  # Check if the merged data exceeds the chunk size
  if row_size * len(merged_list) >= chunk_size:
    # Convert the merged list to a NumPy ndarray and save it to disk as an npz file
    merged_data = np.vstack(merged_list)
    fn = fn_format.format(chunk=chunk_counter)
    np.savez(fn, data=merged_data)

    # Clear the merged list
    merged_list = []

    # Increment the chunk counter
    chunk_counter += 1

  # Get the next row for the dataframe that the selected row came from
  if selected_row['event_type'] == DEPTH_CLEAR_EVENT or selected_row[
      'event_type'] == DEPTH_SNAPSHOT_EVENT:
    snap_row = next(snap_it, (None, None))[1]
  elif selected_row['event_type'] == DEPTH_EVENT:
    depth_row = next(depth_it, (None, None))[1]
  else:
    trades_row = next(trades_it, (None, None))[1]

  if chunk_counter == 2:
    break

# Save any remaining rows to disk
if len(merged_list) > 0:
  # Convert the merged list to a NumPy ndarray and save it to disk as an npz file
  merged_data = np.vstack(merged_list)
  fn = fn_format.format(chunk=chunk_counter)
  np.savez(fn, data=merged_data)
