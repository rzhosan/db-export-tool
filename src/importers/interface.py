import pandas as pd
from contextlib import AbstractContextManager


class DataImporterInterface(AbstractContextManager):
  def read_dataset(self, name: str) -> pd.DataFrame:
    pass

  def overwrite_dataset(self, name: str, data: pd.DataFrame, data_types: dict) -> None:
    pass

  def append_dataset(self, name: str, data: pd.DataFrame, data_types: dict) -> None:
    pass
