
#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-

import rasterio
import os,sys

from tools import *
import glob
from joblib import Parallel, delayed
import pandas as pd
import numpy as np


ScheMa = 'gsa'
CountryInCol = False
cropfieldName = 'cropfield'
codeName = 'original_code'


NameParc = sys.argv[1].lower() # NUTS code 

DirScratch = '/scratch/clamart/'


# textPastCrops = 'r1.c2015'
textPastCrops = ''
textPastNULL = ''

# textPastCrops     = ',r1.c2014'
# textPastNULL = ',NULL as c2014'


ScheMaDum = ScheMa + '_' + NameParc + '_stack'

LaunchPG('CREATE SCHEMA IF NOT EXISTS '+ScheMaDum)
LaunchPG('CREATE SCHEMA IF NOT EXISTS '+ScheMaDum+'_empty')

if Test:
    step    = 10000
    Overlap = 100
    # xmin = 3250000
    # ymin = 1690000
    # xmax = 3290000
    # ymax = 1730000
    xmin = 4840000
    ymin = 2760000
    xmax = 4860000
    ymax = 2780000
    Scale = 10 # in meters
else:
    step    = 50000
    Overlap = 100
    df = pd.concat([GetSQL('select min(st_xmin(geom)) xmin, min(st_ymin(geom)) ymin, max(st_xmin(geom)) xmax, max(st_ymin(geom)) ymax from '+ScheMa+'.'+NameParc+'_'+str(Y)+SuffixInput) for Y in YearList])
    dfmin = df.min(axis=0)
    dfmax = df.max(axis=0)

    xmin =  int(np.fix (dfmin.xmin/step)*step)#.astype(np.int)
    ymin =  int(np.fix (dfmin.ymin/step)*step)#.astype(np.int)
    xmax =  int(np.ceil(dfmax.xmax/step)*step)+step#.astype(np.int)
    ymax =  int(np.ceil(dfmax.ymax/step)*step)+step#.astype(np.int)


StartXvec = np.arange(xmin,xmax,step)-Overlap/2
StartYvec = np.arange(ymin,ymax,step)-Overlap/2

EndXvec = StartXvec + step + Overlap
EndYvec = StartYvec + step + Overlap

TileList = []
for xmin in StartXvec:
    for ymin in StartYvec:
        TileList.append([xmin,ymin])


Diag = np.round(np.sqrt(np.power(Scale,2)*2),decimals=2)

def Rescale_Matrix(img,Min_pixel=10):
    # put a unique value 0-len(np.unique(img)) => better storage
    si = np.shape(img)
    Out = img*0
    U,img,count=np.unique(img,return_counts=True,return_inverse=True)
    Mask = np.isin(img,np.arange(len(U))[count<Min_pixel])#
    img = img[~Mask]
    U,img=np.unique(img,return_inverse=True)
    # import pdb; pdb.set_trace()
    Out[~Mask.reshape(si)] = img+1 # 0 preserve to MAsk
    return Out 

def CreateAllIndex(Layer):
    GenTxt = 'CREATE INDEX ON '+ Layer+' (InDYYYY)'
    IndexText  = 'CREATE INDEX ON '+ Layer+' USING GIST (geom)'
    IndexText += ';' + ';'.join([GenTxt.replace('YYYY',str(Y)).replace('InD','c' ) for Y in YearList])
    IndexText += ';' + ';'.join([GenTxt.replace('YYYY',str(Y)).replace('InD','cf') for Y in YearList])
    if CountryInCol:
        IndexText += ';' + ';'.join([GenTxt.replace('YYYY',str(Y)).replace('InD','n' ) for Y in YearList])  
    return IndexText     

from tqdm.contrib import itertools




def OpenDuckDB(DuckDBpath):
    os.environ["DUCKDB_EXTENSION_PATH"] = "/home/clamart/.duckdb/extensions"
    conn = duckdb.connect(
        DuckDBpath,
        config={
            "allow_unsigned_extensions": "true",
            "extension_directory": os.environ["DUCKDB_EXTENSION_PATH"]
        }
    )
    conn.execute("SET enable_progress_bar = false;")
    conn.execute("INSTALL spatial;")
    conn.execute("LOAD spatial;")
    return conn

def LaunchDuckDB(sql,conn):
    conn.execute(sql)
    conn.commit()

def CloseDuckDB(conn):
    conn.close()

# for xmin, ymin in itertools.product(StartXvec, StartYvec):
    # xmin = int(xmin)
    # ymin = int(ymin)

# def GenSQLcreateTempGSA(Y,Tup):
#     xmin = int(Tup[0])
#     ymin = int(Tup[1])
#     xminymin = str(xmin)+'_'+str(ymin)
#     xminymin = xminymin.replace('-','n')
#     sql = 'create temp table '+NameParc+'_'+str(Y)+'_'+xminymin+' as select cropfield,original_code,geom from '+ScheMa+'.'+NameParc+'_'+str(Y)+SuffixInput+', mypolygon_'+xminymin+' where ST_Intersects(geom,geom_p) ;'
#     sql += 'create index on '+NameParc+'_'+str(Y)+'_'+xminymin+' using GIST(geom);'
#     return sql

                #         'create table '+ScheMa+'.dum as select 2^'+str(iY)+' as value,geom from '+ThisYearLayer)
def LaunchGdalRasterize(Y,xminymin,Frame):
    ThisYearLayer = f'{ScheMa}.{NameParc}_{Y}{SuffixInput}'
    OutFile = f'{DirScratch}_{Y}_{xminymin}.tif'
    commandgd = f"""
    gdal_rasterize -of GTiff -ot UInt32 -a {cropfieldName} -init 0 -te {Frame} \
    -tr {Scale} {Scale} -a_srs EPSG:{epsg} -co BIGTIFF=YES \
    PG:{PgId() } -sql "select geom,{cropfieldName} from {ThisYearLayer}" \
    {OutFile}
    """
    dum = RunSysCommand(commandgd)
    return dum

def ProcTile(Tup):
# if True:
    xmin = int(Tup[0])
    ymin = int(Tup[1])
    # random_uid = str(uuid.uuid4()).replace('-','').replace('.','')
    xminymin = str(xmin)+'_'+str(ymin)
    xminymin = xminymin.replace('-','n')
    Layer = ScheMaDum+'.out_'+xminymin
    try:
    # if True:
        LayerExits = len(GetSQL("SELECT 1 FROM information_schema.columns WHERE table_schema = '"+\
                                ScheMaDum+"' AND table_name   = '"+Layer.split('.')[1]+"' limit 1"))
        if LayerExits==1:
            return Layer
        
        LayerExits = len(GetSQL("SELECT 1 FROM information_schema.columns WHERE table_schema = '"+\
                                ScheMaDum+"_empty' AND table_name   = '"+Layer.split('.')[1]+"' limit 1"))
        if LayerExits==1:
            return Layer
        # print(Tup)
        LayerDum1Exits = len(GetSQL("SELECT 1 FROM information_schema.columns WHERE table_schema = '"+\
                                ScheMaDum+"' AND table_name   = '"+Layer.split('.')[1].replace('out','dum1')+"' limit 1"))
        LayeMappingExits = len(GetSQL("SELECT 1 FROM information_schema.columns WHERE table_schema = '"+\
                                ScheMaDum+"' AND table_name   = '"+Layer.split('.')[1].replace('out','mapping')+"' limit 1"))
        Frame = ' '.join([str(x) for x in [str(xmin),str(ymin),str(xmin+ step + Overlap),str(ymin+ step + Overlap)]])
        # PrintLog(str(xmin)+' | '+str(ymin)+': check if there are parcels',taille=40)
        TextSQL = 'SELECT 1 FROM '+ScheMa+'.'+NameParc+'_YYYY'+SuffixInput+' '\
            'WHERE ST_Intersects(geom, '+\
            'ST_MakeEnvelope('+str(xmin)+','+str(ymin)+','+str(xmin+ step + Overlap)+','+str(ymin+ step + Overlap)+','+epsg+'))'+\
            ' limit 1;'
        # import pdb; pdb.set_trace()
        NbParcelMax = [len(GetSQL(TextSQL.replace('YYYY',str(Y)))) for Y in YearList]
        #
        if np.max(NbParcelMax) == 0:
            # PrintLog(str(xmin)+' | '+str(ymin)+': No parcel',taille=40)
            LaunchPG('create table '+ScheMaDum+'_empty.'+'out_'+xminymin+' (no_col VARCHAR);')
            return None
        # else:
        #     return 1    
        # PrintLog(str(xmin)+' | '+str(ymin)+'',taille=40)
        #
        # if True:
    
        if LayerDum1Exits==0:
            YlistTile = np.array(YearList)[np.array(NbParcelMax)==1]
            # dum = Parallel(len(YlistTile), verbose=0)(delayed(LaunchGdalRasterize)(Y,xminymin,Frame) for Y in YlistTile)
            dum = Parallel(n_jobs=len(YlistTile), prefer="threads", verbose=0)(    delayed(LaunchGdalRasterize)(Y, xminymin, Frame) for Y in YlistTile)

            for iY,Y in enumerate(YlistTile):
                OutFile = f'{DirScratch}_{Y}_{xminymin}.tif'
                if iY==0:
                    Aggregated = OpenWithRasterio(OutFile).astype(np.uint64)
                    si = np.shape(Aggregated)
                    Mask = Aggregated==0
                    Aggregated = Aggregated[~Mask]
                    Aggregated = Rescale_Matrix(Aggregated)
                    Aggregated2 = np.zeros(si,dtype=np.uint64)
                    Aggregated2[~Mask] = Aggregated
                    Aggregated = Aggregated2.reshape(si)
                    del Aggregated2
                    with rasterio.open(OutFile) as ds:
                        profile = ds.profile 
                else:
                    ThisYear = OpenWithRasterio(OutFile).astype(np.uint64)
                    si = np.shape(ThisYear)
                    Mask = ThisYear==0
                    ThisYear = ThisYear[~Mask]
                    ThisYear = Rescale_Matrix(ThisYear)
                    ThisYear2 = np.zeros(si,dtype=np.uint64)
                    ThisYear2[~Mask] = ThisYear
                    ThisYear = ThisYear2.reshape(si)
                    del ThisYear2        
                    Mask = (Aggregated==0) & (ThisYear==0)
                    MaxAgg = Aggregated.max()
                    Aggregated = Aggregated + (MaxAgg + 1) * ThisYear
                    Aggregated = Rescale_Matrix(Aggregated)
                    Aggregated[Mask] = 0
                            # if not Test:
                            #     os.remove(OutFile)

            profile.update(dtype=np.float64)
            with rasterio.open(DirScratch +'Aggregated_'+xminymin+'.tif', 'w', **profile) as dst:
                dst.write(Aggregated.astype(np.float64), 1)
            
            #
            # PrintLog(str(xmin)+' | '+str(ymin)+' | gdal_polygonize',taille=40)
            LaunchPG('drop table if exists '+ScheMaDum+'.dum1_'+xminymin+';')
            commandgd = f"""
                    gdal_polygonize.py \
                    -mask {DirScratch}Aggregated_{xminymin}.tif \
                    {DirScratch}Aggregated_{xminymin}.tif \
                    -f "PostgreSQL" PG:{PgId()} \
                    {ScheMaDum}.dum1_{xminymin}
                """.strip()
            dum = RunSysCommand(commandgd)
            LaunchPG('CREATE INDEX ON '+ScheMaDum+'.dum1_'+xminymin+' USING GIST (wkb_geometry);')
            # import pdb; pdb.set_trace()

            LaunchPG('CREATE INDEX ON '+ScheMaDum+'.dum1_'+xminymin+' (dn);')
            #
            # if not Test:
            os.remove(DirScratch +'Aggregated_'+xminymin+'.tif')
        else:
            PrintLog('dum1 already exists')
    # try:
        # 
        # PrintLog(str(xmin)+' | '+str(ymin)+' | ST_Simplify',taille=40)
        if LayeMappingExits==0:
            # import pdb; pdb.set_trace()
            testIn = Aggregated>0
            Aggregated = Aggregated[testIn]
            (Histo,icsort,IdUnique)=img2cellIndiceOnly(Aggregated)
            A  = [IdUnique]
            for iY,Y in enumerate(YlistTile):
                OutFile = f'{DirScratch}_{Y}_{xminymin}.tif'
                ThisYear = OpenWithRasterio(OutFile).astype(np.uint64)[testIn]
                ValueSplit = img2cellSplitOnly(Histo,icsort,ThisYear)
                stat_out = np.array(list(map(Mediane        ,tuple(ValueSplit[:-1]))))
                A.append(stat_out)
                os.remove(OutFile)


            df = pd.DataFrame(np.array(A).T,columns=['id'] + [f'cf{Y}' for Y in YlistTile ])

            engine = make_engine()
            try:
                df.to_sql(
                    f"mapping_{xminymin}",
                    engine,
                    schema=ScheMaDum,
                    if_exists="replace",
                    index=False,
                    method="multi",
                    chunksize=1000
                )
            finally:
                engine.dispose()

            LaunchPG(f'CREATE INDEX ON {ScheMaDum}.mapping_{xminymin}  (id);')
            ListFields = ''
            for Y in YearList:
                if Y in YlistTile:
                    ListFields += f'NULLIF(m.cf{Y}, 0)::INT4 as cf{Y},' 
                else:
                    ListFields += f'NULL::INT4 as cf{Y},' 
            ListFields  = ListFields[:-1]
            CreateIndex = ';'.join([f'CREATE INDEX ON {ScheMaDum}.out_{xminymin} (cf{Y})' for Y in YearList])
        else:
            PrintLog('dum2 already exists')
        
        sql = f"""
        DROP TABLE IF EXISTS {ScheMaDum}.out_{xminymin}            ;
        CREATE TABLE {ScheMaDum}.out_{xminymin} AS
        SELECT {ListFields},
            ST_Buffer(
                ST_Simplify(
                    ST_SetSRID(wkb_geometry, {epsg}),{Diag}
                ), -{Scale}
            ) AS geom
        FROM {ScheMaDum}.dum1_{xminymin} d
        left join {ScheMaDum}.mapping_{xminymin} m on m.id=d.dn      ;
        CREATE INDEX ON {ScheMaDum}.out_{xminymin} USING GIST (geom);
        DELETE FROM {ScheMaDum}.out_{xminymin} WHERE geom IS NULL OR ST_IsEmpty(geom)  ;
        {CreateIndex}
        """

        LaunchPG(sql)
   
        LaunchPG('drop table if exists '+ ScheMaDum+'.dum1_'+xminymin+';'+\
                'drop table if exists '+ ScheMaDum+'.dum2_'+xminymin+';')
        return Layer
    except Exception as e:
        print(e)
        return 'error ' + Layer
# end loop Tile

# ProcTile(TileList[10])
# print(0)
# t = [2999995.0, 1879995.0]
# ProcTile(t)
# import pdb; pdb.set_trace()

# STOP_HER

# 

def RunMultiProc(NumbPool):
    NumbPool = int(NumbPool)
    PrintLog(f'Loop over the {len(TileList)} Tiles - run on {NumbPool} CPUs',taille=40)
    ListAlreadyProc =  GetSQL("SELECT table_name FROM information_schema.tables WHERE table_schema = '"+ScheMaDum+"' AND table_name like 'out_%%' ")
    ListAlreadyCheckEmpty =  GetSQL("SELECT table_name FROM information_schema.tables WHERE table_schema = '"+ScheMaDum+"_empty' AND table_name like 'out_%%' ")
    TileList2 = []
    for t in TileList:
        if not 'out_'+str(int(t[0]))+'_'+str(int(t[1])) in ListAlreadyProc.table_name.to_list():
            if not 'out_'+str(int(t[0]))+'_'+str(int(t[1])) in ListAlreadyCheckEmpty.table_name.to_list():
                TileList2.append(t)
    if NumbPool>1:
        # AllLayers = Parallel(NumbPool, verbose=5)(delayed(ProcTile)(t) for t in TileList2)
        AllLayers = Parallel(n_jobs=NumbPool, verbose=0)(delayed(ProcTile)(t) for t in tqdm(TileList2))
    else:
        AllLayers = [ProcTile(t) for t in TileList2]
    return AllLayers

# t = [3759995.0,3079995.0]
# ProcTile(t)

# if True:
    # AllLayers = [ProcTile(t) for t in TileList]
if True:
    if Test:
        NumbPool = 6
    else:
        NumbPool = max(6,int(50/len(YearList)))
    # ProcTile(TileList[0])
    # import cloudpickle
    # cloudpickle.dumps(ProcTile)
    AllLayers = RunMultiProc(NumbPool)

    if any(s.startswith('error') for s in list(filter(None, AllLayers))):
        NumbPool = 8
        AllLayers = RunMultiProc(NumbPool)

    if any(s.startswith('error') for s in list(filter(None, AllLayers))):
        NumbPool = 6
        AllLayers = RunMultiProc(NumbPool)

    PrintLog('ProcTile done',taille=40)

    if any(s.startswith('error') for s in list(filter(None, AllLayers))):
        for l in AllLayers:
            if l.startswith('error'):
                print(l)
        STILL_SOME_ERRORS

if True:

    ListAlreadyProc =  GetSQL("SELECT table_name FROM information_schema.tables WHERE table_schema = '"+ScheMaDum+"' AND table_name like 'out_%%' ").table_name.to_list()

    textcol = ','.join(['cf'+str(y) for y in YearList])
    CreateIndex = ';'.join([f'CREATE INDEX ON {ScheMaDum}.dum4 (cf{Y})' for Y in YearList])
    Union = " UNION ".join(
            f"SELECT {textcol}, geom FROM {ScheMaDum}.{layer}"
            for layer in ListAlreadyProc  )
    PrintLog('Merge Tiled Layers',taille=40)
    sql = f"""
        DROP TABLE IF EXISTS {ScheMaDum}.dum4;
        CREATE TABLE {ScheMaDum}.dum4 AS {Union};

        """
        # -- CREATE INDEX ON {ScheMaDum}.dum4 USING GIST (geom);
        # -- {CreateIndex};
    LaunchPG(sql)



DuckDBpath = f"/scratch/clamart/process_stack.duckdb"


if True:
    if os.path.exists(DuckDBpath):
        os.remove(DuckDBpath)

conn = OpenDuckDB(DuckDBpath)

AllColumns2  = ','.join([f'cf{Y} ' for Y in YearList])
if True:

    sql_pg = f"""
    SELECT
        {AllColumns2},
        ST_AsBinary(geom) AS geom
    FROM {ScheMaDum}.dum4
    """

    # Plain DataFrame with geom as bytes/WKB
    df = pd.read_sql(sql_pg, get_engine())

    conn.register("df", df)

    sql = f"""
    DROP TABLE IF EXISTS  dum4 ;
    CREATE TABLE dum4 AS
        SELECT {AllColumns2} , ST_GeomFromWKB(geom) geom
        FROM df;   
    """
    LaunchDuckDB(sql,conn)

    for Y in YearList:
        sql = f" SELECT cropfield::INT4,original_code FROM gsa.{NameParc}_{Y} ; "
        df = GetSQL(sql)
        conn.register("df_view", df)
        LaunchDuckDB(f"CREATE TABLE {NameParc}_{Y} AS SELECT * FROM df_view",conn)
        # LaunchDuckDB(sql,conn)
else:
    conn = OpenDuckDB(DuckDBpath)

PrintLog('st_union',taille=40)

AllColumns1  = ','.join([f'cf{Y}::INT4 as cf{Y}' for Y in YearList])

# CreateIndex = ';'.join([f'CREATE INDEX ON {LayerOut} (cf{Y})' for Y in YearList])

LayerOutDuck = LayerOut.replace('.','_')
sql = f"""
DROP TABLE IF EXISTS {LayerOutDuck};
CREATE TABLE {LayerOutDuck} AS
SELECT 
    ST_Union_Agg(geom) AS geom,
    {AllColumns1}
FROM dum4
GROUP BY {AllColumns2};
"""
LaunchDuckDB(sql,conn)

# {CreateIndex};
# CREATE INDEX ON {LayerOutDuck} using gist(geom);
# LaunchPG(sql)

# LaunchDuckDB('drop table if exists '+ScheMaDum+'.dum4')

PrintLog('attach original_code',taille=40)

for Y in YearList:
    sql = f"""
    ALTER table {LayerOutDuck} ADD COLUMN c{Y} VARCHAR;
    UPDATE {LayerOutDuck} s SET c{Y} = g.original_code
    FROM {NameParc}_{Y} g WHERE g.cropfield= s.cf{Y};
    """
    # print(sql)
    LaunchDuckDB(sql,conn)



PrintLog('Final processing',taille=40)
# {CreateIndex};
# ALTER TABLE {LayerOutDuck} ADD COLUMN cropfield SERIAL PRIMARY KEY;
# CREATE INDEX ON {LayerOutDuck} (cropfield);
# CREATE INDEX ON {LayerOutDuck} (area_ha);
sql = f"""
ALTER TABLE {LayerOutDuck}  ADD COLUMN cropfield INT4;
CREATE SEQUENCE cropfield_seq START 1;
UPDATE {LayerOutDuck} SET cropfield = nextval('cropfield_seq');
ALTER TABLE {LayerOutDuck} ADD COLUMN area_ha REAL;
UPDATE {LayerOutDuck} SET area_ha = ST_Area(geom) / 10000;
DELETE FROM {LayerOutDuck} where area_ha<{FilterOut/ 10000};
"""
# print(sql)
LaunchDuckDB(sql,conn)
# LaunchPG( ';'.join(['drop table '+Layer for Layer in AllLayers] ))



LaunchPG(f'DROP TABLE IF EXISTS {LayerOut};')
conn_str = (
    f"host={postgis_host} "
    f"port={postgis_port} "
    f"dbname={postgis_dbname} "
    f"user={postgis_user} "
    f"password={postgis_password}"
)

AllColumns1  = ','.join([f'cf{Y} INT4 ' for Y in YearList])
AllColumns1 += ','
AllColumns1 += ','.join([f'c{Y} VARCHAR ' for Y in YearList])

AllColumns2  = ','.join([f'cf{Y} ' for Y in YearList])
AllColumns2 += ','
AllColumns2 += ','.join([f'c{Y} ' for Y in YearList])

sql = f"""
ATTACH '{conn_str}' AS pg_temp (TYPE postgres);
DROP TABLE IF EXISTS pg_temp.{LayerOut};
CREATE TABLE pg_temp.{LayerOut} (
    cropfield BIGINT,
    {AllColumns1},
    area_ha FLOAT,
    geom_wkb BYTEA
);
INSERT INTO pg_temp.{LayerOut}
SELECT
    cropfield,
    {AllColumns2},
    area_ha,
    ST_AsWKB(geom) AS geom_wkb
FROM {LayerOutDuck};
"""
LaunchDuckDB(sql,conn)

CloseDuckDB(conn)

CreateIndex  = ';'.join([f'CREATE INDEX ON {LayerOut} (cf{Y})' for Y in YearList])
CreateIndex += ';'
CreateIndex += ';'.join([f'CREATE INDEX ON {LayerOut} (c{Y})' for Y in YearList])
CreateIndex += f'; CREATE INDEX ON {LayerOut} using GIST(geom);'
CreateIndex += f'; CREATE INDEX ON {LayerOut} (area_ha);'
CreateIndex += f'; CREATE INDEX ON {LayerOut} (cropfield);'
sql = f"""
ALTER TABLE {LayerOut} ADD COLUMN geom geometry(MultiPolygon, {epsg});
UPDATE {LayerOut} SET geom = ST_SetSRID(ST_GeomFromWKB(geom_wkb), {epsg});
ALTER TABLE {LayerOut} DROP COLUMN geom_wkb;
{CreateIndex}
"""
LaunchPG(sql)

if True:
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    for Y in YearList:
        df = GetSQL('select round(st_x(st_centroid(geom))) x, round(st_y(st_centroid(geom))) y from '+ ScheMa+'.'+NameParc+'_'+str(Y) )
        coord = np.round(df/1000).groupby(['x','y']).mean().reset_index()
        coord *= 1000
        ax.plot(coord.x.to_numpy(),coord.y.to_numpy(),'.', markersize=1)
    df = GetSQL('select round(st_x(st_centroid(geom))) x, round(st_y(st_centroid(geom))) y from '+ LayerOut )
    coord = np.round(df/1000).groupby(['x','y']).mean().reset_index()
    coord *= 1000
    ax.plot(coord.x.to_numpy(),coord.y.to_numpy(),'.k', markersize=1)
    for t in ListAlreadyProc:
        xmin = int(t.split('_')[1])
        ymin = int(t.split('_')[2])
        x1, y1 = xmin, ymin
        x2, y2 = xmin + step, ymin
        x3, y3 = xmin + step, ymin + step
        x4, y4 = xmin, ymin + step
        x_coords = [x1, x2, x3, x4, x1]  # Repeat the first point to close the square
        y_coords = [y1, y2, y3, y4, y1]  # Repeat the first point to close the square
        plt.plot(x_coords, y_coords, 'k')
        plt.fill(x_coords, y_coords, color='grey', alpha=0.5)
    # print('Processing Grid resolution = '+str(round(step/1000))+'km')
    if interactive:
        print('close the map window and reply to the question: Is the map complete?')
        plt.show()
        inp = input('Is the map complete? Y or N\n')
    else:
        inp = 'y'
        plt.savefig(f"{LayerOut}.png", dpi=300, bbox_inches='tight')
    plt.close()
else:
    inp = 'y'

if inp.lower()[0]=='y':
    LaunchPG( 'DROP SCHEMA IF EXISTS '+ScheMaDum+' CASCADE;')
    LaunchPG( 'DROP SCHEMA IF EXISTS '+ScheMaDum+'_empty CASCADE;')
# LaunchPG('DROP SCHEMA IF  EXISTS '+ScheMaDum)
