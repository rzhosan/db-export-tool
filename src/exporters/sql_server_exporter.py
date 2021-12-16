import pandas as pd
import pyodbc
from exporters.interface import DataExporterInterface
from models.data_types import DataTypes
import utils.env as env

class SqlServerDataExporter(DataExporterInterface):
  """A data exporter implementation that allow to export all tables from SQL Server instance"""

  def __init__(self) -> None:  
    driver = env.get_optional('SQL_DRIVER', 'ODBC Driver 17 for SQL Server')
    host = env.get_required('SQL_HOST')
    port = env.get_optional('SQL_PORT', '1433')
    database = env.get_required('SQL_DATABASE')
    username = env.get_required('SQL_USERNAME')
    password = env.get_required('SQL_PASSWORD')

    self.connection_string = f'Driver={{{driver}}};Server={host},{port};Database={database};UID={username};PWD={password}'

  def __enter__(self):
    self.sql_connection = pyodbc.connect(self.connection_string)
    return self
    
  def __exit__(self, exc_type, exc_value, exc_tb):
    self.sql_connection.close()

  def get_all_datasets(self, config: dict) -> pd.DataFrame:
    df = pd.read_sql("SELECT * FROM INFORMATION_SCHEMA.TABLES;", self.sql_connection)

    if 'table' in config:
      df = df.loc[df['TABLE_NAME'] == config['table']]

    return df[['TABLE_SCHEMA', 'TABLE_NAME']].rename({
      'TABLE_SCHEMA': 'schema',
      'TABLE_NAME': 'name'
    }, axis=1)
  

  def read_dataset(self, dataset: pd.DataFrame):
    """Reads the whole dataset into panda's dataframe and return the data_types for each column too"""
    df = pd.read_sql(f'SELECT * FROM {dataset["schema"]}.{dataset["name"]};', self.sql_connection)
    column_types = self.__get_column_types(dataset["schema"], dataset["name"])

    return df, column_types

  def __get_column_types(self, schema, table):
    columns = pd.read_sql(f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{schema}'", self.sql_connection)
    data_types = {}

    for _, column in columns.iterrows():
      name = column['COLUMN_NAME']
      type = column['DATA_TYPE']
      data_types[name] = data_types_dict[type]

    return data_types


data_types_dict = {
  'int': DataTypes.INT,
  'bigint': DataTypes.BIGINT,
  'binary': DataTypes.BYTE_ARRAY,
  'bit': DataTypes.BOOL,
  'char':	DataTypes.STRING,
  'date':	DataTypes.DATE,
  'datetime': DataTypes.DATETIME,
  'decimal':	DataTypes.DECIMAL,
  'float':	DataTypes.FLOAT,
  'nchar':	DataTypes.STRING,
  'nvarchar':	DataTypes.STRING,
  'nvarchar(max)':	DataTypes.STRING,
  'real':	DataTypes.FLOAT,
  'smalldatetime':	DataTypes.DATETIME,
  'smallint':	DataTypes.INT,
  'tinyint':	DataTypes.INT,
  'uniqueidentifier':	DataTypes.STRING,
  'varbinary':	DataTypes.STRING,
  'varbinary(max)':	DataTypes.BYTE_ARRAY,
  'varchar': DataTypes.STRING,
  'varchar(n)':	DataTypes.STRING,
  'varchar(max)':	DataTypes.STRING,
}
