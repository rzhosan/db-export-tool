import pandas as pd
import utils.env as env
from models.data_types import DataTypes
from sqlalchemy import create_engine, types
from importers.interface import DataImporterInterface


class MySqlImporter(DataImporterInterface):
  def __init__(self) -> None:
    self.__connection_string = env.get_required('MYSQL_CONNECTION_STRING')

  def __enter__(self):
    self.__sql_connection = create_engine(self.__connection_string)
    return self

  def __exit__(self, exc_type, exc_value, exc_tb):
    pass

  def overwrite_dataset(
      self,
      name: str,
      data: pd.DataFrame,
      data_types: dict,
      **kwargs: dict,
  ) -> None:
    dtype = self.__map_date_types(data_types)
    data.to_sql(name, con=self.__sql_connection, if_exists='replace', index=False, dtype=dtype)

  def append_dataset(
      self,
      name: str,
      data: pd.DataFrame,
      data_types: dict,
      **kwargs: dict,
  ) -> None:
    dtype = self.__map_date_types(data_types)
    data.to_sql(name, con=self.__sql_connection, if_exists='append', index=False, dtype=dtype)

  def __map_date_types(self, data_types):
    result = {}

    for name in data_types:
      result[name] = data_types_dict[data_types[name]]

    return result


data_types_dict = {
    DataTypes.INT: types.INTEGER,
    DataTypes.BIGINT: types.BIGINT,
    DataTypes.BYTE_ARRAY: types.BINARY,
    DataTypes.BOOL: types.BOOLEAN,
    DataTypes.STRING:	types.NVARCHAR(255),
    DataTypes.FLOAT: types.FLOAT,
    DataTypes.DOUBLE: types.REAL,
    DataTypes.DECIMAL: types.DECIMAL,
    DataTypes.DATE: types.DATE,
    DataTypes.DATETIME: types.DATETIME,
    DataTypes.JSONB: types.JSON,
}
