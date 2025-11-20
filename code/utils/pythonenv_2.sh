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


conda create -y --prefix /mnt/jeoproc/envs/REFOCUS/EuroCropsV2 python=3.10
conda install -y --prefix /mnt/jeoproc/envs/REFOCUS/EuroCropsV2 pandas==1.3.5 geopandas==0.12.1 'sqlalchemy==1.4.46' ipykernel==6.7.0 psycopg2-binary==2.9.2 pyarrow -c conda-forge
conda install geoalchemy2

conda install -y --prefix /mnt/jeoproc/envs/REFOCUS/EuroCropsV2 -c conda-forge duckdb=0.8.1
echo "To activate the virtual environment, run: source eurocropsv2/bin/activate"
echo "To use the conda environment, run: conda activate /mnt/jeoproc/envs/REFOCUS/EuroCropsV2"  
deactivate
