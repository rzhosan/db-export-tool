from exporters.interface import DataExporterInterface
from exporters.sql_server_exporter import SqlServerDataExporter
from exporters.pgsql_exporter import PgSqlDataExporter


def get_exporter(name: str) -> DataExporterInterface:
  if name == 'sql_server':
    return SqlServerDataExporter()
  elif name == 'postgresql':
    return PgSqlDataExporter()
    return
  else:
    raise AttributeError(f'No data exporter found with name \'{name}\'')
