import sys,os
import pandas as pd
import numpy as np
from tqdm import tqdm

from tools import *

sql = r"SELECT table_name t FROM information_schema.columns WHERE table_schema = 'gsa' and table_name like '%%\_20__' group by table_name;"
df = GetSQL(sql)

List = ['pt_2023']
df = df[df.t.isin(List)]


OutDir = '/eos/jeodpp/data/projects/REFOCUS/clamart/data/cheap/InvalidGeom/'+dt.datetime.now().strftime('%Y%m%d%H%M')+'/'

MakeNewDir(OutDir)


if True:
    PrintLog( 'check and save invalid geom')
    B=[]
    List = np.sort(df.t.unique())
    c = ''
    for t in tqdm(List):

        sql = 'select count(*) as input from gsa.'+t+' where not st_isvalid(geom)'
        dum = GetSQL(sql)
        dum['table'] = t
        B.append(dum)
        if len(dum)>0:
            sql = 'drop table if exists invalidgeom.'+t+';create table invalidgeom.'+t+' as select *,0 as method from gsa.'+t+' where not st_isvalid(geom);create index on invalidgeom.'+t+' (cropfield);create index on invalidgeom.'+t+' (method);create index on invalidgeom.'+t+' using GIST(geom);'
            LaunchPG(sql)

    dfcountinvalid = pd.concat(B)

    dfcountinvalid.to_csv(OutDir+'invalid_geom.csv')
else:
    dfcountinvalid = pd.read_csv(OutDir+'invalid_geom.csv')

if True: # run ST_MakeValid

    PrintLog( 'ST_MakeValid')
    dfcountinvalid['MakeValid'] = 0

    TabList = dfcountinvalid[dfcountinvalid.input>0].table.tolist()

    for table in TabList:
        PrintLog(table)
        try:
            LaunchPG('UPDATE gsa.'+table+' SET geom = ST_MakeValid(ST_Multi(ST_CollectionExtract(ST_MakeValid(geom), 3))) WHERE not st_isvalid(geom); ')
            LaunchPG('UPDATE invalidgeom.'+table+' SET method = 1 WHERE st_isvalid(geom); ')
        except:
            ListCF = GetSQL('select cropfield from gsa.'+table+' where not st_isvalid(geom)').cropfield.tolist()
            for cf in ListCF:
                try:
                    LaunchPG('UPDATE gsa.'+table+' SET geom = ST_MakeValid(ST_Multi(ST_CollectionExtract(ST_MakeValid(geom), 3))) WHERE cropfield='+str(cf)+'; ')
                    LaunchPG('UPDATE invalidgeom.'+table+' SET method = 1 WHERE cropfield='+str(cf))
                except:
                    pass

        sql = 'select count(*) c from gsa.'+table+' where not st_isvalid(geom)'
        dum = GetSQL(sql)

        dfcountinvalid.loc[dfcountinvalid.table==table,'MakeValid'] = dum.c.iloc[0]

    dfcountinvalid.to_csv(OutDir+'invalid_geom_ST_MakeValid.csv')
else:
    dfcountinvalid = pd.read_csv(OutDir+'invalid_geom_ST_MakeValid.csv')


if True: # run buffer(0)

    PrintLog( 'buffer0')
    dfcountinvalid['buffer0'] = 0

    TabList = dfcountinvalid[dfcountinvalid.MakeValid>0].table.tolist()

    for table in TabList:
        PrintLog(table)
        try:
            LaunchPG('UPDATE gsa.'+table+' SET geom = buffer(geom,0) WHERE not st_isvalid(geom); ')
            LaunchPG('UPDATE invalidgeom.'+table+' SET method = 3 WHERE st_isvalid(geom); ')
        except:
            ListCF = GetSQL('select cropfield from gsa.'+table+' where not st_isvalid(geom)').cropfield.tolist()
            fails = 0
            print('Run buffer0 on '+table+' / total= '+str(len(ListCF)))
            for cf in ListCF:
                try:
                    LaunchPG('UPDATE gsa.'+table+' SET geom = buffer(geom,0) WHERE cropfield='+str(cf)+'; ')
                    LaunchPG('UPDATE invalidgeom.'+table+' SET method = 3 WHERE cropfield='+str(cf))
                except:
                    fails +=1
            print('still Failed on '+table+' / N= '+str(fails))
        sql = 'select count(*) c from gsa.'+table+' where not st_isvalid(geom)'
        dum = GetSQL(sql)

        dfcountinvalid.loc[dfcountinvalid.table==table,'buffer0'] = dum.c.iloc[0]

    dfcountinvalid.to_csv(OutDir+'invalid_geom_buffer0.csv')

if True: # run buffer(1)

    PrintLog( 'buffer1')
    dfcountinvalid['buffer1'] = 0

    TabList = dfcountinvalid[dfcountinvalid.buffer0>0].table.tolist()

    for table in TabList:
        PrintLog(table)
        try:
            LaunchPG('UPDATE gsa.'+table+' SET geom = buffer(buffer(geom,-1),1) WHERE not st_isvalid(geom); ')
            LaunchPG('UPDATE invalidgeom.'+table+' SET method = 4 WHERE st_isvalid(geom); ')
        except:
            ListCF = GetSQL('select cropfield from gsa.'+table+' where not st_isvalid(geom)').cropfield.tolist()
            fails = 0
            print('Run buffer1 on '+table+' / total= '+str(len(ListCF)))
            for cf in ListCF:
                try:
                    LaunchPG('UPDATE gsa.'+table+' SET geom = buffer(buffer(geom,-1),1) WHERE cropfield='+str(cf)+'; ')
                    LaunchPG('UPDATE invalidgeom.'+table+' SET method = 4 WHERE cropfield='+str(cf))
                except:
                    LaunchPG('UPDATE invalidgeom.'+table+' SET method = 9 WHERE cropfield='+str(cf))
                    fails +=1
            print('still Failed on '+table+' / N= '+str(fails))
        sql = 'select count(*) c from gsa.'+table+' where not st_isvalid(geom)'
        dum = GetSQL(sql)

        dfcountinvalid.loc[dfcountinvalid.table==table,'buffer1'] = dum.c.iloc[0]

        LaunchPG('UPDATE invalidgeom.'+table+' SET method = 9 WHERE not st_isvalid(geom); ')

        ListCF = GetSQL('select cropfield from gsa.'+table+' where not st_isvalid(geom)').cropfield.tolist()
        ListCF = ' '.join([str(cf) for cf in ListCF])

        dfcountinvalid['remaining_cropfield'] = ''
        dfcountinvalid.loc[dfcountinvalid.table==table,'remaining_cropfield'] = ListCF

    dfcountinvalid.to_csv(OutDir+'invalid_geom_buffer1.csv')

