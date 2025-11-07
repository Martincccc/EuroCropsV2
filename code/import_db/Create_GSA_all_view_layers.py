

import os
from git import Repo
import csv
import pandas as pd
import numpy as np
from glob import glob
import shutil
import psycopg2
import os,sys
from joblib import Parallel, delayed
import time

DirShared = os.environ['DirCode']+'/Shared/'
sys.path.append(DirShared)
from sqlalchemy import create_engine

from ..processing.tools import *

from glob import glob


ScheMa = postgis_cfg['schema']


DirInScr = config_dict['path']['fastio_dir']

dfout=[]


def count_trailing_zeros(text):
    """
    Count the number of trailing zeros in a given string.
    :param text: The input string to check for trailing zeros.
    :return: The count of trailing zeros.
    """
    zero_count = 0
    for char in reversed(text):
        if char == '0':
            zero_count += 1
        else:
            break
    return zero_count

# List = list(range(2023,2008-1,-1))
List = list(range(2008,2023+1))
List = [year for pair in zip(List[:len(List)//2], reversed(List[len(List)//2:])) for year in pair]
List = [2008, 2023, 2009, 2010, 2011, 2021, 2012, 2019,2020, 2013, 2018, 2014,2022, 2017, 2015, 2016]
List = [2023]
# List = [2023,2017,2021,2018,2022,2020,2019]
# List = list(range(2008,2017+1))
# List.remove(2022)

# List = [2023]
# List = list(range(2019,2023+1))

DicSelCrops = {
    3310000000:'arable_crops',
    3310100000:'cereal',
    3310101000:'common_soft_wheat',
    3310102000:'durum_hard_wheat',
    3310103000:'rye',
    3310104000:'barley',
    3310106000:'maize_corn_popcorn',
    3310107000:'rice',
    3310108000:'triticale',
    3310116000:'sorghum',
    3310200000:'legumes',
    3310202010:'alfalfa_lucerne',
    3310300000:'potatoes',
    3310604000:'oilseed_crops',
    3310604010:'rapeseed_rape',
    3310604020:'sunflower',
    3310700000:'fresh_vegetables',
    3310719010:'sugar_beet',
    3310800000:'flowers_ornamental_plants',
    3320000000:'grassland_grass',
    3320100000:'permanent_grassland',
    3320200000:'temporary_grassland',
    3330000000:'permanent_crops_perennial',
    3330100000:'orchards_fruits',
    3330300000:'nuts',
    3330400000:'citrus',
    3330500000:'olive',
    3330600000:'vineyards_wine_vine_rebland_grapes',
    3350000000:'greenhouse_foil_film',
    3360000000:'tree_wood_forest',
    3370000000:'unmaintained',
    3390000000:'not_known_and_other'}


sql_find = """
        SELECT table_name 
        FROM information_schema.columns 
        WHERE table_name LIKE '%%_YeaR' 
        AND table_schema = 'gsa'
        AND column_name = 'original_code'
    """

sql_join = """
    create table out.TaBle_hcat4 as
    SELECT 'NuTs' AS nuts,cropfield,
    geom,e.hcat4_code,u.usage_code,e.original_code,area_ha FROM gsa.TaBle g
    left join cropmapping.eurocrops e on e.original_code like g.original_code and e.nuts='NuTs' 
    left join cropmapping.eurocrops_usage u on g.original_code=u.original_code and u.nuts='NuTs';
    CREATE INDEX ON out.TaBle_hcat4 USING GIST (geom); 
    CREATE INDEX ON out.TaBle_hcat4 (area_ha);  
    CREATE INDEX ON out.TaBle_hcat4 (hcat4_code);  
    CREATE INDEX ON out.TaBle_hcat4 (original_code);  
    CREATE INDEX ON out.TaBle_hcat4 (usage_code);  
       """

sql_join_A = """
CREATE UNLOGGED TABLE out.TaBle_hcat4 (
  nuts varchar(3),
  cropfield BIGINT,  
  geom GEOMETRY,
  hcat4_code BIGINT,
  usage_code smallint,
  original_code varchar,
  area_ha FLOAT(1)
);
       """

sql_join_B = """
INSERT INTO out.TaBle_hcat4
SELECT
  'NuTs' AS nuts,
  g.cropfield,
  g.geom,
  e.hcat4_code,
  u.usage_code,
  e.original_code,
  g.area_ha
FROM gsa.TaBle g
LEFT JOIN cropmapping.eurocrops e
  ON e.original_code = g.original_code AND e.nuts = 'NuTs'
LEFT JOIN cropmapping.eurocrops_usage u
  ON g.original_code = u.original_code and u.nuts='NuTs'
WHERE g.cropfield BETWEEN Min_Id AND Max_Id;
       """

sql_join_C = """
    CREATE INDEX ON out.TaBle_hcat4 USING GIST (geom); 
    CREATE INDEX ON out.TaBle_hcat4 (area_ha);  
    CREATE INDEX ON out.TaBle_hcat4 (hcat4_code);  
    CREATE INDEX ON out.TaBle_hcat4 (original_code);  
    CREATE INDEX ON out.TaBle_hcat4 (usage_code);  
       """


sql_process = open('./create_gsa_view2.sql', "r").read()

sql_crop = """
    drop table if exists grid.grid_gsa_YeaR_CroP cascade;
    create table grid.grid_gsa_YeaR_CroP as 
    select grd_id,sum(area_ha) area_ha,geom from grid.grid_gsa_YeaR 
    where floor(hcat4_code/FacTor)=ShortCode
    group by grd_id,geom ;
    CREATE INDEX ON grid.grid_gsa_YeaR_CroP USING GIST (geom); 
    CREATE INDEX ON grid.grid_gsa_YeaR_CroP (grd_id);  
    CREATE INDEX ON grid.grid_gsa_YeaR_CroP (area_ha);
    """



def Proc(Y):
# for Y in [2021]:
    # if True:
    try:
        PrintLog(str(Y))
        # SQL_file = './create_gsa_view.sql' # this version crashes when too big
        # Command = open(SQL_file, "r").read()
        # Command = Command.replace('YeaR', str(Y))
        # LaunchPG(Command)    

        ListTable = GetSQL(sql_find.replace('YeaR', str(Y))).table_name.tolist()
        
        CommandList = []
        for t in ListTable:
            dum = GetSQL(\
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "+\
                "WHERE table_schema = 'out' AND table_name = '"+t+"_hcat4');")
            TableExists = dum.iloc[0,0]
            if not TableExists:
                if False:
                    dum = GetSQL("SELECT MIN(cropfield) mmin, MAX(cropfield) mmax FROM gsa."+t+";")
                    # import pdb ; pdb.set_trace() 
                    min_id = int(dum.mmin.iloc[0])
                    max_id = int(dum.mmax.iloc[0])
                    # if max_id - min_id > 3e6:
                    PrintLog(f'create {t}_hcat4 in batchs')
                    BATCH_SIZE = int(3e6)
                    LaunchPG(sql_join_A.replace('TaBle', t).replace('NuTs', t.split('_')[0]))
                    for start_id in range(min_id, max_id + 1, BATCH_SIZE):
                        LaunchPG(sql_join_B.replace('TaBle', t).replace('NuTs', t.split('_')[0]).replace('Min_Id', str(min_id)).replace('Max_Id', str(max_id)))
                    LaunchPG(sql_join_C.replace('TaBle', t).replace('NuTs', t.split('_')[0]))
                else:
                    PrintLog(f'prepare {t}_hcat4')
                    Command = sql_join.replace('TaBle', t).replace('NuTs', t.split('_')[0])
                    # LaunchPG(Command)
                    CommandList.append(Command)
        PrintLog(f'Launch All SQL')
        dum = Parallel(4, verbose=5)(delayed(LaunchPG)(Command) for Command in CommandList) 
        
        sql_union = 'drop table if exists out.gsa_YeaR ; ' + \
            'CREATE table out.gsa_YeaR AS ' + \
            ' UNION ALL '.join(['select * from out.'+t+'_hcat4' for t in ListTable]) + ';' +\
            'CREATE INDEX ON out.gsa_YeaR USING GIST (geom); ' +\
            'CREATE INDEX ON out.gsa_YeaR (hcat4_code);  ' +\
            'CREATE INDEX ON out.gsa_YeaR (original_code);  ' +\
            'CREATE INDEX ON out.gsa_YeaR (area_ha);' 
        sql_drop = ';'.join(['drop table out.'+t+'_hcat4' for t in ListTable]) 

        LaunchPG(sql_union.replace('YeaR', str(Y)))
        LaunchPG(sql_drop)
        LaunchPG(sql_process.replace('YeaR', str(Y)))

        PrintLog(str(Y) + ' crop layers')
        CommandList = []
        for c in DicSelCrops.keys():
            num0 = count_trailing_zeros(str(c))
            Command = sql_crop.replace('YeaR', str(Y)) \
                        .replace('CroP', DicSelCrops[c]) \
                        .replace('FacTor', str(np.power(10,num0))) \
                        .replace('ShortCode', str(c)[:-num0]) 
            CommandList.append(Command)
            # LaunchPG(Command)

        dum = Parallel(4, verbose=5)(delayed(LaunchPG)(Command) for Command in CommandList) 
        PrintLog('DONE '+ str(Y))
    except:
        time.sleep(5)
        PrintLog('failed on '+ str(Y))

OUT = Parallel(4, verbose=5)(delayed(Proc)(Y) for Y in List)

# Proc(2017)


# OUT = Parallel(2, verbose=5)(delayed(Proc)(Y) for Y in List)

# for Y in List:
#     Proc(Y)