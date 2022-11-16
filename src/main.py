from exporters import get_exporter
from importers import get_importer
from timeit import default_timer as timer
import logging
from utils import env

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')


def export(config={}):
  errors = 0
  max_errors = 5
  exporter_name = config.get('exporter') or env.get_required('EXPORTER_NAME')
  importer_name = config.get('importer') or env.get_required('IMPORTER_NAME')
  partition_column = config.get('partition_column', env.get_optional('PARTITION_COLUMN'))
  partition_value = config.get('partition_value', env.get_optional('PARTITION_VALUE'))

  with get_exporter(exporter_name) as exporter, get_importer(importer_name) as importer:
    all_datasets = exporter.get_all_datasets(config).sort_values('name')
    logging.info(f'Found {all_datasets["name"].count()} datasets. Exporting them...')

    for _, dataset in all_datasets.iterrows():
      try:
        logging.info(f'Exporting dataset {dataset["name"]}')
        start = timer()
        dfs, data_types = exporter.read_dataset(dataset)

        records_count = 0

        for df in dfs:
          if partition_column:
            df[partition_column] = partition_value

          if records_count == 0:
            importer.overwrite_dataset(dataset["name"], df, data_types, partition_column)
          else:
            importer.append_dataset(dataset["name"], df, data_types, partition_column)

          records_count += len(df)

        if records_count == 0:
          logging.info(f'No records exported for dataset {dataset["name"]}')
        else:
          elapsed = int(timer() - start)
          logging.info(
              f'Successfully exported {records_count} records of dataset {dataset["name"]} within {elapsed} seconds')
      except Exception as e:
        logging.error(f'An error occurred on importing of {dataset["name"]}')
        logging.error(e, exc_info=True)
        errors += 1
        if errors >= max_errors:
          logging.fatal('Too many errors occurred. Exiting...')
          return


if __name__ == '__main__':
  export()
