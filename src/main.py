from exporters import get_exporter
from importers import get_importer
from timeit import default_timer as timer
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def export(config={}):
  errors = 0
  max_errors = 5

  with get_exporter('sql_server') as exporter, get_importer('s3_parquet') as importer:
    all_datasets = exporter.get_all_datasets(config).sort_values('name')
    logging.info(f'Found {all_datasets["name"].count()} datasets. Exporting them...')

    for _, dataset in all_datasets.iterrows():
      try:
        logging.info(f'Exporting dataset {dataset["name"]}')
        start = timer()
        dfs, data_types = exporter.read_dataset(dataset)

        records_count = 0

        for df in dfs:
          if records_count == 0:
            importer.overwrite_dataset(dataset["name"], df, data_types)
          else:
            importer.append_dataset(dataset["name"], df, data_types)

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
