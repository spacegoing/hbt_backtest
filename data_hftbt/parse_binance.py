import struct
import pandas as pd
import dask.dataframe as dd
import dask.array as da
from dask import delayed
import time
from hftbacktest.reader import TRADE_EVENT, DEPTH_EVENT, DEPTH_CLEAR_EVENT, DEPTH_SNAPSHOT_EVENT


# Read the CSV files into Dask DataFrames
def parse_one_day(date='2022-10-31'):
  snap_df = dd.read_csv(
      f'./orderbook/csv/BTCUSDT_T_DEPTH_{date}_depth_snap.csv')
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

  total_df = dd.concat([snap_df, depth_df, trades_df],
                       axis=0,
                       ignore_index=True,
                       interleave_partitions=True)

  #*
  def gen_key(row):
    # Snapshot b clear> snapshot a clear> Snapshot b normal > snapshot a normal
    # >
    # snapshot a/b set = trade a/b set

    # Sort key= timestamp + 1 b clear > 2 a clear > 3 b snapnormal > 4 a snapnormal > 5 the other
    try:
      if row['event_type'] == DEPTH_CLEAR_EVENT:
        if row['side'] == 1:
          return struct.pack('ll', int(row['exch_time']), 1)
        else:
          return struct.pack('ll', int(row['exch_time']), 2)
      elif row['event_type'] == DEPTH_SNAPSHOT_EVENT:
        if row['side'] == 1:
          return struct.pack('ll', int(row['exch_time']), 3)
        else:
          return struct.pack('ll', int(row['exch_time']), 4)
      else:
        return struct.pack('ll', int(row['exch_time']), 5)
    except Exception as e:
      import pdb
      pdb.set_trace()

  total_df['sort_key'] = total_df.apply(gen_key, axis=1, meta=('sort_key', 'O'))

  total_df = total_df.set_index('sort_key')
  total_df_sorted = total_df.map_partitions(lambda x: x.sort_index())

  total_df_sorted = total_df_sorted[[
      'event_type', 'exch_time', 'local_time', 'side', 'price', 'qty'
  ]]

  total_df_sorted.to_csv('total_sorted.csv', index=False)

  print(total_df.head(10))

  #*
