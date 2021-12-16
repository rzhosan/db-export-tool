import pandas as pd
import pandas as pd
from models.data_types import DataTypes
from contextlib import AbstractContextManager

class DataImporterInterface(AbstractContextManager):
  def save_dataset(self, name: str, dataset: pd.DataFrame, data_types):
    """Saves the whole panda's dataframe"""
    pass

  def has_dataset(self, name: str):
    pass
