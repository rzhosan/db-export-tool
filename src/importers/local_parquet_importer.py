import os
import shutil
from datetime import datetime
import pandas as pd
import utils.env as env
from importers.interface import DataImporterInterface


class LocalParquetImporter(DataImporterInterface):
  def __init__(self) -> None:
    self.__folder = env.get_optional('IMPORT_FOLDER', '../.data/local_parquet_importer')
    self.__chunk_size = int(env.get_optional('PARQUET_CHUNK_SIZE', '10000'))

  def read_dataset(self, name: str) -> pd.DataFrame:
    folder = os.path.join(self.__folder, name)
    df = pd.read_parquet(folder) if os.path.exists(folder) and os.listdir(folder) else pd.DataFrame()

    return df

  def overwrite_dataset(self, name: str, data: pd.DataFrame, data_types: dict):
    folder = os.path.join(self.__folder, name)

    if os.path.exists(folder):
      shutil.rmtree(folder)

    os.mkdir(folder)

    data.to_parquet(os.path.join(folder, f'{name}.parquet'))

  def append_dataset(self, name: str, data: pd.DataFrame, data_types: dict):
    folder = os.path.join(self.__folder, name)

    if not os.path.exists(folder):
      os.mkdir(folder)

    file_name = f'{datetime.now().timestamp()}.parquet'
    data.to_parquet(os.path.join(folder, file_name))

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, exc_tb):
    pass
