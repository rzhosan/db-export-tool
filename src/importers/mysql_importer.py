import pandas as pd
import awswrangler as wr
import boto3
import utils.env as env
from models.data_types import DataTypes
from sqlalchemy import create_engine
from importers.interface import DataImporterInterface


class MySqlImporter(DataImporterInterface):
  def __init__(self) -> None:
    self.__connection_string = env.get_required('MYSQL_CONNECTION_STRING')
    self.__chunk_size = int(env.get_optional('CHUNK_SIZE', '50_000'))

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
      partition_column: str = None,
  ) -> None:
    if data.empty:
      # wr.s3.delete_objects(f'{self.s3_path}/{name}')
      return

    # dtype = self.__map_date_types(data_types)
    data.to_sql(name, con=self.__sql_connection, if_exists='replace')

  def append_dataset(
      self,
      name: str,
      data: pd.DataFrame,
      data_types: dict,
      partition_column: str = None,
  ) -> None:
    # dtype = self.__map_date_types(data_types)
    data.to_sql(name, con=self.__sql_connection, if_exists='append')

  def __map_date_types(self, data_types):
    result = {}

    for name in data_types:
      result[name] = data_types_dict[data_types[name]]

    return result


data_types_dict = {
    DataTypes.INT: 'int',
    DataTypes.BIGINT: 'bigint',
    DataTypes.BYTE_ARRAY: 'binary',
    DataTypes.BOOL: 'boolean',
    DataTypes.STRING:	'string',
    DataTypes.FLOAT: 'float',
    DataTypes.DOUBLE: 'double',
    DataTypes.DECIMAL: 'double',
    DataTypes.DATE: 'date',
    DataTypes.DATETIME: 'timestamp',
}
