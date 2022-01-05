from typing import Iterable
import pandas as pd
from contextlib import AbstractContextManager


class DataExporterInterface(AbstractContextManager):
  def get_all_datasets(self, config: dict) -> pd.DataFrame:
    """Returns all datasets to import based on config. E.g., """
    pass

  def read_dataset(self, dataset_info: pd.Series) -> Iterable:
    """Reads the pwhole dataset into panda's dataframe and return the data_types for each column too"""
    pass

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, exc_tb):
    pass
