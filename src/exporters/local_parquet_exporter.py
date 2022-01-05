import os
from typing import Iterable
import pandas as pd
import utils.env as env
from exporters.interface import DataExporterInterface


class LocalParquetExporter(DataExporterInterface):
  def __init__(self) -> None:
    self.__folder = env.get_optional('LOCAL_PARQUET_EXPORTER_FOLDER', '../.data/local_parquet_exporter')

  def get_all_datasets(self, config: dict) -> pd.DataFrame:
    folders = os.listdir(self.__folder) if os.path.exists(self.__folder) else []

    if 'table' in config:
      folders = filter(lambda x: x == config['table'], folders)

    return pd.DataFrame(map(lambda x: {'name': x}, folders))

  def read_dataset(self, dataset_info: pd.Series) -> Iterable:
    folder = os.path.join(self.__folder, dataset_info['name'])
    df = pd.read_parquet(f'{folder}/')

    return [df], None

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, exc_tb):
    pass
