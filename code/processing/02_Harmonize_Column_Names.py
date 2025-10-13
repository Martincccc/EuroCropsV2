


import numpy as np
import pandas as pd
import os,shutil
from glob import glob
from tools import *

from joblib import Parallel, delayed

FileList = '../../data/processing/columns_listing.csv'
Columns = pd.read_csv(FileList)
Columns['table_name']  =  Columns['nuts'].str.lower() + "_" + Columns['year'].astype(str)

ScheMa = 'gsa'

TableList = Columns.table_name.unique()

NoProc = False

for i,t in enumerate(TableList):
  
    TableExists = GetSQL("SELECT 1 FROM information_schema.tables WHERE table_schema = '"+ScheMa+"' and table_name='"+t+"'")
    if len(TableExists)==0:
        continue
    PrintLog(t,taille=20)

    cdum = Columns[(Columns['table_name']==t)&(Columns['ToKeep']==1)]

    SQL = ''

    ColNames = GetSQL(" SELECT * from "+ScheMa+"."+t+' limit 2').columns

    for index, item in cdum.iterrows():
        # import pdb ; pdb.set_trace()
        colnamecase = ColNames[next((i for i, col in enumerate(ColNames) if col.lower() == item.column_name.lower()), -1)]
        if item.type != item.column_name.lower():
            SQL+='ALTER TABLE '+ScheMa+'.'+t+' RENAME COLUMN "'+colnamecase+'" TO '+item.type+'; '
    
    geomname = GetSQL("SELECT f_geometry_column FROM geometry_columns WHERE f_table_schema = '"+ScheMa+"'   AND f_table_name = '"+t+"'").iloc[0,0]
    
    if geomname != 'geom':
        SQL+='ALTER TABLE '+ScheMa+'.'+t+' RENAME COLUMN '+geomname+' TO geom'+'; '

    PrimKey = GetSQL(" SELECT a.attname AS column_name  FROM   pg_index i JOIN   pg_attribute a ON a.attrelid = i.indrelid "+\
                     " AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = '"+ScheMa+"."+t+"'::regclass AND    i.indisprimary; ")

    if len(PrimKey)==1:
        PrimKey = PrimKey.iloc[0,0]
        SQL+='ALTER TABLE '+ScheMa+'.'+t+' RENAME COLUMN '+PrimKey+' TO cropfield'+'; '
    else:
        SQL+='ALTER table '+ScheMa+'.'+t+' ADD COLUMN cropfield SERIAL PRIMARY KEY'+'; '

    GeomIndexExists = False
    dum = GetSQL("SELECT indexname FROM pg_indexes WHERE schemaname='"+ScheMa+"' and tablename = '"+t+"'")
    for indX in list(dum.indexname):
        if 'pkey' in indX: 
            SQL+='ALTER INDEX '+ScheMa+'."'+indX+'" RENAME TO '+t+'_cropfield_pkey'+'; '
        else:
            if 'geom' in indX:
                if geomname != 'geom':
                    SQL+='ALTER INDEX '+ScheMa+'.'+indX+' RENAME TO '+indX.replace(geomname,'geom')+'; '
                GeomIndexExists = True
            else:
                SQL+='DROP INDEX '+ScheMa+'.'+indX+'; '

    for index, item in cdum.iterrows():
        if (item.type != 'off_area') & (item.type != 'off_id'):
            SQL+='CREATE INDEX ON '+ScheMa+'.'+t+' ('+item.type+')'+'; '

    SQL+='ALTER TABLE '+ScheMa+'.'+t+' ADD COLUMN area_ha real'+'; '
    SQL+='UPDATE '+ScheMa+'.'+t+' SET area_ha = round(ST_Area(geom))/10000 '+'; '
    SQL+='CREATE INDEX ON '+ScheMa+'.'+t+' (area_ha)'+'; '
    if not GeomIndexExists:
        SQL+='CREATE INDEX ON '+ScheMa+'.'+t+' using GIST (geom)'
    
    LaunchPG(SQL)





















