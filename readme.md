# Db Exporting Tool

This is the repository for a database exporting tool.

- Currently, the following databases are supported:
  - Local parquet, PostgreSQL, SQL Server as source databases (to copy data from)
  - Local parquet, s3 + Athena, MySQL as the destination databases (to copy data to)

## Docker

Available via djosani/db-export-tool:1.0.0.

## Prerequisites

Although, it's possible to use any python environment, the instructions cover the anaconda usage.

To work locally, open condas terminal, go to a project folder, create a new virtual environments and install necessary packages:

```
conda create -n db-export python=3.10 -y
conda activate db-export
conda config --env --add channels conda-forge
conda install ipykernel pyodbc -y
pip install -r requirements.txt
```

Then restart VSCode. When you're going to be running python notebooks, use the `db-export` environment for this project (top right corner, when you open notebook files).
