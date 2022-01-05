from exporters.interface import DataExporterInterface
from exporters.sql_server_exporter import SqlServerDataExporter


def get_exporter(name: str) -> DataExporterInterface:
  if name == 'sql_server':
    return SqlServerDataExporter()
  else:
    raise AttributeError(f'No data exporter found with name \'{name}\'')
