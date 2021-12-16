import pandas as pd
import pandas as pd
from models.data_types import DataTypes
from contextlib import AbstractContextManager

class DataExporterInterface(AbstractContextManager):
  def get_all_datasets(self, config: dict) -> pd.DataFrame:
    """Returns all datasets to import based on config. E.g., """
    pass

  def read_dataset(self, dataset: pd.DataFrame):
    """Reads the pwhole dataset into panda's dataframe and return the data_types for each column too"""
    pass
