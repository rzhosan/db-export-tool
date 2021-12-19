import pandas as pd
import awswrangler as wr
import boto3
import utils.env as env
from models.data_types import DataTypes
from importers.interface import DataImporterInterface

class S3ParquetImporter(DataImporterInterface):
  def __init__(self) -> None:
    self.s3_path = env.get_required('S3_PATH')
    self.glue_database_name = env.get_required('GLUE_DATABASE_NAME')
    self.boto3_session = boto3.Session(region_name=env.get_required('AWS_REGION'))

  def __enter__(self):
    return self
    
  def __exit__(self, exc_type, exc_value, exc_tb):
    pass

  def save_dataset(self, name: str, dataset: pd.DataFrame, data_types) -> None:
    """Saves the whole panda's dataframe"""
    dtype = self.__map_date_types(data_types)

    wr.s3.to_parquet(
      df=dataset,
      path=f'{self.s3_path}/{name}',
      dataset=True,
      dtype=dtype,
      index=False,
      database=self.glue_database_name,
      table=name,
      boto3_session=self.boto3_session,
      schema_evolution=True,
      mode='overwrite',
    )

  def has_dataset(self, name: str):
    return wr.catalog.does_table_exist(database=self.glue_database_name, table=name, boto3_session=self.boto3_session)

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