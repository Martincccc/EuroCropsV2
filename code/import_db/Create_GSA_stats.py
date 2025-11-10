

#  source /storage/$USER/venv/martin_jeolab_env//bin/activate

import sys,os

from joblib import Parallel, delayed

# from joblib import Parallel, delayed
# from simpledbf import Dbf5

from sqlalchemy import create_engine
import psycopg2

import pandas as pd
import numpy as np
import subprocess
import datetime as dt 
from tqdm import tqdm

from ..processing.tools import *
ScheMa = postgis_cfg['schema'] 

LayerName = 'out.all_stats' 

sql = """
insert into LaYerNaMe
select nuts,YeaR as year,hcat4_code,usage_code, sum(area_ha) area_ha
from out.gsa_YeaR_centroid g 
group by nuts,hcat4_code,usage_code;
"""

# LaunchPG('ALTER TABLE '+LayerName+' ADD COLUMN hcat4_code int8;'+\
# 'UPDATE '+LayerName+' a SET hcat4_code = h.hcat4_code FROM cropmapping.hcat4 h WHERE a.hcat4_name = h.hcat4_name;'+\
# 'ALTER TABLE '+LayerName+' DROP COLUMN hcat4_name;')

for i, year in enumerate(range(2008, 2024)):
    # if year<2019:
    #     continue
    PrintLog(str(year))
    if i==0:
        dum = 'drop table if exists LaYerNaMe;create table LaYerNaMe as '
        LaunchPG(sql.replace('insert into LaYerNaMe',dum).replace('YeaR',str(year)).replace('LaYerNaMe',LayerName))
    else:
        LaunchPG(sql.replace('YeaR',str(year)).replace('LaYerNaMe',LayerName))

sql = """
create index on LaYerNaMe (nuts) ;
create index on LaYerNaMe (year) ;
create index on LaYerNaMe (hcat4_code) ;
create index on LaYerNaMe (usage_code) ;
create index on LaYerNaMe (area_ha) ;
"""
LaunchPG(sql.replace('LaYerNaMe',LayerName))

LaunchPG('ALTER TABLE '+LayerName+' ADD COLUMN hcat4_name VARCHAR(64);'+\
'UPDATE '+LayerName+' a SET hcat4_name = h.hcat4_name FROM cropmapping.hcat4 h WHERE a.hcat4_code = h.hcat4_code;')

LaunchPG('ALTER TABLE '+LayerName+' ADD COLUMN usage_name VARCHAR(64);'+\
'UPDATE '+LayerName+' a SET usage_name = h.usage_name FROM cropmapping.usage h WHERE a.usage_code = h.usage_code;')

LaunchPG('create index on '+LayerName+' (hcat4_name) ;')
LaunchPG('create index on '+LayerName+' (usage_name) ;')