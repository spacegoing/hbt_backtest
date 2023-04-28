import tarfile
import gzip
import numpy as np
import zipfile
import pandas as pd
from hftbacktest.data.utils import binancefutures
from hftbacktest.data.utils import create_last_snapshot

import os
import re
from datetime import datetime


#* trades filenames
# List all files in the data folder
def get_filepath_list(data_folder, file_prefix):
  files = os.listdir(data_folder)
  filtered_files = [f for f in files if f.startswith(file_prefix)]

  # Function to convert filename to date
  def filename_to_date(filename: str) -> datetime:
    date_string = filename[len(file_prefix):].split(".")[0]
    return datetime.strptime(date_string, "%Y-%m-%d")

  # Sort files by date
  sorted_files = sorted(filtered_files, key=filename_to_date)

  # Add the data folder path to the sorted file names
  sorted_file_paths = [os.path.join(data_folder, file) for file in sorted_files]
  return sorted_file_paths


data_folder = "./trades"
file_prefix = "BTCUSDT-trades-"
trades_fp_list = get_filepath_list(data_folder, file_prefix)
data_folder = "./orderbook"
file_prefix = "BTCUSDT_T_DEPTH_"
orderbook_fp_list = get_filepath_list(data_folder, file_prefix)
print(trades_fp_list)
print(orderbook_fp_list)

#* Data Preparation
binancefutures.convert(
    'usdm/btcusdt_20230405.dat.gz', output_filename='btcusdt_20230405')

# Eod Orderbook Snapshot

# Build 20230404 End of Day snapshot. It will be used for the initial snapshot for 20230405.
create_last_snapshot(
    'btcusdt_20230404.npz',
    tick_size=0.01,
    lot_size=0.001,
    output_snapshot_filename='btcusdt_20230404_eod')

# Build 20230405 End of Day snapshot.
# Due to the file size limitation, btcusdt_20230405.npz does not contain data for the entire day.
create_last_snapshot(
    'btcusdt_20230405.npz',
    tick_size=0.01,
    lot_size=0.001,
    initial_snapshot='btcusdt_20230404_eod.npz',
    output_snapshot_filename='btcusdt_20230405_eod')

#* Viz binance orderbook data
archive = zipfile.ZipFile('./trades/BTCUSDT-trades-2022-11-01.zip', 'r')
text = archive.open('BTCUSDT-trades-2022-11-01.csv')
for i, l in enumerate(text.readlines()):
  print(l)
  if i > 10:
    break

#* Viz binance trades data
fpath = './orderbook/BTCUSDT_T_DEPTH_2022-11-01.tar.gz'
# df = pd.read_csv('./orderbook/BTCUSDT_T_DEPTH_2022-11-01_depth_snap.csv', header=0)

data_dict = dict()
with tarfile.open(fpath, 'r:gz') as tar:
  for member in tar.getmembers():
    tmplist = []
    if member.isfile() and member.name.endswith('.csv'):
      file = tar.extractfile(member)
      i = 0
      while True:
        l = file.readline().decode('utf-8')
        tmplist.append(l.strip().split(','))
        i += 1
        if l and i > 12000000:
          break
      data_dict[member.name] = tmplist

#*
from hftbacktest.data.utils import tardis

data = tardis.convert(['BTCUSDT_trades.csv.gz', 'BTCUSDT_book.csv.gz'])

#* Example: Viz Tardis Data
a = []
with gzip.open('BTCUSDT_book.csv.gz', 'r') as f:
  # with gzip.open('BTCUSDT_trades.csv.gz', 'r') as f:
  aa = f.readlines()
# a.append(line.decode('utf-8'))
d = []
# for a in aa:
#   d.append(a.decode('utf-8').split(','))
# if 'true' not in line.decode('utf-8'):
#   print(line)
#   if i > 10:
#     break
# print(line)
# if i>100:
#   break

#* Example: Viz hftb Binance Stream Data
# binancefutures.convert(
#     'usdm/btcusdt_20230405.dat.gz', output_filename='btcusdt_20230405')
with gzip.open('BTCUSDT_T_DEPTH_2022-11-01.tar.gz', 'r') as f:
  for i in range(1200):
    line = f.readline()
    if i > 1000:
      print(line)

df = pd.DataFrame(
    data,
    columns=[
        'event', 'exch_timestamp', 'local_timestamp', 'side', 'price', 'qty'
    ])
df['event'] = df['event'].astype(int)
df['exch_timestamp'] = df['exch_timestamp'].astype(int)
df['local_timestamp'] = df['local_timestamp'].astype(int)
df['side'] = df['side'].astype(int)
print(df)

#* Example: Viz Eod
df = pd.DataFrame(
    data,
    columns=[
        'event', 'exch_timestamp', 'local_timestamp', 'side', 'price', 'qty'
    ])
df['event'] = df['event'].astype(int)
df['exch_timestamp'] = df['exch_timestamp'].astype(int)
df['local_timestamp'] = df['local_timestamp'].astype(int)
df['side'] = df['side'].astype(int)
print(df)
