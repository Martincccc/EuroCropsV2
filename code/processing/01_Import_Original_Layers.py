

import numpy as np
import pandas as pd
import os,shutil
from tools import *

from glob import glob

from joblib import Parallel, delayed


ScheMa = postgis_cfg['schema']

FileList = '../../EuroCropsV2/data/processing/import_list_original_datasets.csv'
df = pd.read_csv(FileList)

FileList = '../../data/processing/columns_listing.csv'
df_columns = pd.read_csv(FileList)

DirInEos = config_dict['path']['originaldataset_dir'] # where are located the orifginal data set
DirInScr = config_dict['path']['fastio_dir']

dfout=[]


def ProcRow(row):
    PrintLog(row['name'])
    CheckTable= GetSQL("SELECT EXISTS (SELECT FROM information_schema.tables "+\
                "WHERE  table_name     = '"+row['name']+"' "+\
                "AND    table_schema   = '"+ScheMa+"'); ")

    filename1 = DirInEos+row.Nuts+'/'+row.path
    filename2 = DirInScr   +   row['path'].split('/')[-1]
    if len(glob(filename2))==0:

        if filename1[-4:]=='.shp':
            for f in glob(filename1[:-4]+'.*'):
                
                shutil.copy(f,DirInScr+f.split('/')[-1])
        else:
            shutil.copy(filename1,filename2)

    Command = 'ogrinfo '+filename2+' -dialect sqlite -sql "select count(*) c from '+row.layer+'" '
    out = os.popen(Command).read()


    N_original = int(out.split(" ")[-1].replace('\n',''))

    ListCol = ','.join(df_columns[(df_columns.nuts==row.Nuts)&(df_columns.year==row.year)&(df_columns.ToKeep==1)].column_name.tolist())

    if CheckTable.iloc[0,0]==False:
        try:

            Command = 'ogr2ogr -f "PostgreSQL" -t_srs EPSG:3035 -makevalid -skipfailures '+\
                      'PG:"host='+postgis_cfg['host']+' port='+postgis_cfg['port']+' user='+postgis_cfg['user']+\
                      ' dbname='+postgis_cfg['dbname']+' password='+postgis_cfg['password']+'" '+filename2+\
                      ' '+row.layer+' -nln '+row['name']+\
                      ' -nlt MULTIPOLYGON -lco SCHEMA="'+ScheMa+\
                        '" -lco OVERWRITE=YES -lco precision=NO -dim XY -select "'+ListCol+'"'


            os.system(Command)

            N_final = GetSQL('select count(*) c from '+ScheMa+'.'+row['name']).iloc[0,0]

            if N_final==N_original:
                PrintLog(row['name'].ljust(10)+ ': IMPORTED FULL',taille=40)
            else:
                PrintLog(row['name'].ljust(10)+ ': IMPORTED but FAILED on ' +str(np.round(100-N_final/N_original*100,decimals=4))+'% ('+str(N_original-N_final)+'/'+str(N_original)+')',taille=40)
        except:
            PrintLog(row['name'].ljust(10)+ ': FAILED',taille=40)
            N_final = np.nan
    else:
        N_final = GetSQL('select count(*) c from '+ScheMa+'.'+row['name']).iloc[0,0]
        if N_final==N_original:
            PrintLog(row['name'].ljust(10)+ ': ALREADY PROC. FULL',taille=40)
        else:
            PrintLog(row['name'].ljust(10)+ ': ALREADY PROC. '+str(np.round(100-N_final/N_original*100,decimals=4))+'% ('+str(N_final-N_original)+'/'+str(N_original)+')',taille=40)
            print('DROP TABLE '+ScheMa+'.'+row['name']+';')
    return [row['name'],N_original,N_final,N_final/N_original]


dfout= Parallel(8, verbose=0,timeout=99999)(delayed(ProcRow)(row) for index, row in df.iterrows())


dfout = pd.DataFrame(dfout,columns=['table','N_original','N_final','Ratio'])

dfout.to_csv(DirInEos+'Import_report_'+dt.datetime.now().strftime('%Y%m%d%H%M')+'.csv')
