from exporters import get_exporter
from importers import get_importer

errors = 0
max_errors = 20

def export(config = {}):
  Exporter = get_exporter('sql_server')
  Importer = get_importer('s3_parquet')

  with Exporter() as exporter, Importer() as importer:
    all_datasets = exporter.get_all_datasets(config)
    print(f'Found {all_datasets["name"].count()} datasets. Exporting them...')

    for _, dataset in all_datasets.iterrows():
      try:
        print(f'Exporting dataset {dataset["schema"]}.{dataset["name"]}')
        df, data_types = exporter.read_dataset(dataset)
        if not df.empty:
          importer.save_dataset(dataset["name"], df, data_types)
      except Exception as e:
        print(f'An error occurred on importing of {dataset["schema"]}.{dataset["name"]}')
        print(e)
        errors = errors + 1
        if errors >= max_errors:
          print("Too many errors occurred. Exiting...")
          return

if __name__ == '__main__':
  export()
