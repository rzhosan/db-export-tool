from importers.interface import DataImporterInterface
from importers.s3_parquet_importer import S3ParquetImporter
from importers.mysql_importer import MySqlImporter


def get_importer(name: str) -> DataImporterInterface:
  if name == 's3_parquet':
    return S3ParquetImporter()
  elif name == "mysql":
    return MySqlImporter()
  else:
    raise AttributeError(f'No data importer found with name \'{name}\'')
