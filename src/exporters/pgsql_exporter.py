from typing import Iterable
import pandas as pd
import psycopg2
from exporters.interface import DataExporterInterface
from models.data_types import DataTypes
import utils.env as env


class PgSqlDataExporter(DataExporterInterface):
  """A data exporter implementation that allow to export all tables from a PostgreSQL instance"""

  def __init__(self) -> None:
    self.__connection_string = env.get_required('POSTGRESQL_CONNECTION_STRING')
    self.__chunk_size = int(env.get_optional('CHUNK_SIZE', '50_000'))

  def __enter__(self):
    self.__sql_connection = psycopg2.connect(self.__connection_string)
    return self

  def __exit__(self, exc_type, exc_value, exc_tb):
    self.__sql_connection.close()

  def get_all_datasets(self, config: dict) -> pd.DataFrame:
    df = pd.read_sql(
        "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA NOT IN ('pg_catalog', 'information_schema');",
        self.__sql_connection
    )

    if 'table' in config:
      df = df.loc[df['table_name'] == config['table']]

    return df[['table_schema', 'table_name']].rename({
        'table_schema': 'schema',
        'table_name': 'name'
    }, axis=1)

  def read_dataset(self, dataset: pd.Series) -> Iterable:
    """Reads the whole dataset into panda's dataframe and return the data_types for each column too"""
    column_types = self.__get_column_types(dataset['schema'], dataset['name'])
    df = pd.read_sql(f'SELECT * FROM {dataset["schema"]}.{dataset["name"]};',
                     self.__sql_connection, chunksize=self.__chunk_size)

    return df, column_types

  def __get_column_types(self, schema, table):
    columns = pd.read_sql(
        f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{schema}'",
        self.__sql_connection
    )

    data_types = {}

    for _, column in columns.iterrows():
      name = column['column_name']
      type = column['data_type']
      data_types[name] = data_types_dict[type]

    return data_types


data_types_dict = {
    'integer': DataTypes.INT,
    'character varying': DataTypes.STRING,
    'timestamp without time zone': DataTypes.DATETIME,
    'boolean': DataTypes.BOOL,
    'numeric': DataTypes.DOUBLE,
    'USER-DEFINED': DataTypes.STRING,
}
