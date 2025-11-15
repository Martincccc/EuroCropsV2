#!/bin/bash
python3 -m venv --system-site-packages euc_export
euc_export/bin/python3 -m pip install --upgrade pip
source euc_export/bin/activate
pip install -U pandas==1.3.5
pip install -U geopandas==0.12.1
pip install -U 'sqlalchemy==1.4.46'
pip install -U ipykernel==6.7.0
pip install -U psycopg2-binary==2.9.2
pip install -U pyarrow
