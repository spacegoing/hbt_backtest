import zipfile
import os
import tarfile
import glob
from concurrent.futures import ThreadPoolExecutor


def extract_tar_gz(file_path, output_path):
  with tarfile.open(file_path, 'r:gz') as tar:
    tar.extractall(path=output_path)


def uncompress_orderbook_files(folder_path, subfolder_name):
  output_path = os.path.join(folder_path, subfolder_name)

  if not os.path.exists(output_path):
    os.makedirs(output_path)

  tar_files = glob.glob(os.path.join(folder_path, '*.tar.gz'))

  with ThreadPoolExecutor() as executor:
    for tar_file in tar_files:
      executor.submit(extract_tar_gz, tar_file, output_path)


def uncompress_trades_files(folder_path, subfolder_name):
  output_path = os.path.join(folder_path, subfolder_name)

  if not os.path.exists(output_path):
    os.makedirs(output_path)

  tar_files = glob.glob(os.path.join(folder_path, '*.zip'))

  with ThreadPoolExecutor() as executor:
    for tar_file in tar_files:
      executor.submit(extract_tar_gz, tar_file, output_path)
      with zipfile.ZipFile(tar_file, 'r') as archive:
        archive.extractall(output_path)


if __name__ == '__main__':
  # folder_path = './orderbook'
  # sub_name = 'csv'
  # uncompress_orderbook_files(folder_path, sub_name)

  folder_path = './trades'
  sub_name = 'csv'
  uncompress_trades_files(folder_path, sub_name)
