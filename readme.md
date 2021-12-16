# Db Exporting Tool
This is the repository for a database exporting tool.
- Currently, only the SQL Server to AWS S3 + Glue export type is supported.
- A tool allows to capture data changes and store them in a specific table format
- Data is stored in a `parquet` format

## Prerequisites
To work locally, open condas terminal, go to a project folder, create a new virtual environments and install necessary packages:

```
conda create -n db-export python=3.10 -y
conda activate db-export
conda config --env --add channels conda-forge
conda install ipykernel pyodbc -y
pip install -r requirements.txt
```

Then restart VSCode. When you're going to be running python notebooks, use the `db-export` environment for this project (top right corner, when you open notebook files).
