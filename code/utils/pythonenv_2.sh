#!/bin/bash
python3 -m venv --system-site-packages eurocropsv2
eurocropsv2/bin/python3 -m pip install --upgrade pip
source eurocropsv2/bin/activate
pip install -U pandas==1.3.5
pip install -U geopandas==0.12.1
pip install -U 'sqlalchemy==1.4.46'
pip install -U ipykernel==6.7.0
pip install -U psycopg2-binary==2.9.2
pip install -U pyarrow
pip install -U duckdb==1.1.3
pip install -U geoalchemy2==0.18.0

# HERE INLCLUDE DUCKDB INSTALLATION
# wget https://install.duckdb.org/v1.1.3/duckdb_cli-linux-amd64.zip
# unzip duckdb_cli-linux-amd64.zip
# mv ...

