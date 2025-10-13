
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

epsg      = '3035' # EPSG of the rasters
Scale     = 2      # 2m grid scale
FilterOut = 1000   # 0.1 ha
step      = 30000  # 30km tiles
Overlap   = 100    # 100m overlap

SuffixInput = ''


PrintLog(NameParc,taille=40)
PrintLog(NameParc,taille=40)
PrintLog(NameParc,taille=40)


sql = r"SELECT SPLIT_PART(table_name,'_',2) FROM information_schema.columns "+\
    r"WHERE table_schema = '{}' AND table_name LIKE '{}_20__{}' group by table_name".format(ScheMa, NameParc,SuffixInput)

YearList = [int(x) for x in list(np.squeeze(np.sort(GetSQL(sql).to_numpy())))]


LayerOut = 'stack.'+NameParc+'_stack'
LayerOut = ScheMa+'.'+NameParc+'_stack'

DirScratch = '/scratch/clamart/'


textPastCrops = ''
textPastNULL = ''

df = pd.concat([GetSQL('select min(st_xmin(geom)) xmin, min(st_ymin(geom)) ymin, max(st_xmin(geom)) xmax, max(st_ymin(geom)) ymax from '+ScheMa+'.'+NameParc+'_'+str(Y)+SuffixInput) for Y in YearList])
dfmin = df.min(axis=0)
dfmax = df.max(axis=0)

ScheMaDum = ScheMa + '_' + NameParc + '_stack'

LaunchPG('CREATE SCHEMA IF NOT EXISTS '+ScheMaDum)
LaunchPG('CREATE SCHEMA IF NOT EXISTS '+ScheMaDum+'_empty')

# step    = 10000
# Overlap = 1000
xmin =  int(np.fix (dfmin.xmin/step)*step)#.astype(np.int)
ymin =  int(np.fix (dfmin.ymin/step)*step)#.astype(np.int)
xmax =  int(np.ceil(dfmax.xmax/step)*step)#.astype(np.int)
ymax =  int(np.ceil(dfmax.ymax/step)*step)#.astype(np.int)


StartXvec = np.arange(xmin,xmax,step)-Overlap/2
StartYvec = np.arange(ymin,ymax,step)-Overlap/2

EndXvec = StartXvec + step + Overlap
EndYvec = StartYvec + step + Overlap

TileList = []
for xmin in StartXvec:
    for ymin in StartYvec:
        TileList.append([xmin,ymin])

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


def ProcTile(Tup):
    xmin = int(Tup[0])
    ymin = int(Tup[1])
    xminymin = str(xmin)+'_'+str(ymin)
    xminymin = xminymin.replace('-','n')
    Layer = ScheMaDum+'.dum3_'+xminymin
    try:
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
                                ScheMaDum+"' AND table_name   = '"+Layer.split('.')[1].replace('dum3','dum1')+"' limit 1"))
        LayerDum2Exits = len(GetSQL("SELECT 1 FROM information_schema.columns WHERE table_schema = '"+\
                                ScheMaDum+"' AND table_name   = '"+Layer.split('.')[1].replace('dum3','dum2')+"' limit 1"))
        Frame = ' '.join([str(x) for x in [str(xmin),str(ymin),str(xmin+ step + Overlap),str(ymin+ step + Overlap)]])
        PrintLog(str(xmin)+' | '+str(ymin)+': check if there are parcels',taille=40)
        TextSQL = 'SELECT 1 FROM '+ScheMa+'.'+NameParc+'_YYYY'+SuffixInput+' '\
            'WHERE ST_Intersects(geom, '+\
            'ST_MakeEnvelope('+str(xmin)+','+str(ymin)+','+str(xmin+ step + Overlap)+','+str(ymin+ step + Overlap)+','+epsg+'))'+\
            ' limit 1;'
        # import pdb; pdb.set_trace()
        NbParcelMax = [len(GetSQL(TextSQL.replace('YYYY',str(Y)))) for Y in YearList]
        #
        if np.max(NbParcelMax) == 0:
            PrintLog(str(xmin)+' | '+str(ymin)+': No parcel',taille=40)
            LaunchPG('create table '+ScheMaDum+'_empty.'+'dum3_'+xminymin+' (no_col VARCHAR);')
            return None
        # else:
        #     return 1    
        PrintLog(str(xmin)+' | '+str(ymin)+'',taille=40)
        #
        # if True:
    
        if LayerDum1Exits==0:
            for iY,Y in enumerate(np.array(YearList)[np.array(NbParcelMax)==1]):
                Ys = str(Y)
                # PrintLog(str(xmin)+' | '+str(ymin)+' | Ingest '+Ys,taille=40)
                ThisYearLayer = ScheMa+'.'+NameParc+'_'+Ys+SuffixInput
                OutFile = DirScratch +'ThisYear_'+xminymin+'.tif'
                commandgd = 'gdal_rasterize -of GTiff -ot UInt32 -a '+cropfieldName+' -init 0 -te ' + Frame + ' ' + \
                    ' -tr '+str(Scale)+' '+str(Scale)+' -a_srs EPSG:'+epsg+' -co BIGTIFF=YES '  +\
                    'PG:' + PgId() + ' -sql "select geom,'+cropfieldName+' from ' + ThisYearLayer+ '" '+\
                    OutFile
                # print(commandgd)
                dum = RunSysCommand(commandgd)
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
                    # import pdb; pdb.set_trace()
                    MaxAgg = Aggregated.max()
                    Aggregated = Aggregated + (MaxAgg + 1) * ThisYear
                    Aggregated = Rescale_Matrix(Aggregated)
                    Aggregated[Mask] = 0
            # PrintLog(str(xmin)+' | '+str(ymin)+' | Write Outout Raster',taille=40)
            with rasterio.open(OutFile) as ds:
                profile = ds.profile 
            #
            profile.update(dtype=np.float64)
            #
            with rasterio.open(DirScratch +'Aggregated_'+xminymin+'.tif', 'w', **profile) as dst:
                dst.write(Aggregated.astype(np.float64), 1)
            os.remove(OutFile)

            LaunchPG('drop table if exists '+ScheMaDum+'.dum1_'+xminymin+';')
            commandgd = 'gdal_polygonize.py -mask '+DirScratch +'Aggregated_'+xminymin+'.tif'+' '+DirScratch +'Aggregated_'+xminymin+'.tif'+\
                ' -f ''PostgreSQL'' PG:'+PgId()+' '+ ScheMaDum+'.'+'dum1_'+xminymin+' '
            dum = RunSysCommand(commandgd)
            LaunchPG('CREATE INDEX ON '+ScheMaDum+'.dum1_'+xminymin+' USING GIST (wkb_geometry);')
            #
            os.remove(DirScratch +'Aggregated_'+xminymin+'.tif')
        else:
            PrintLog('dum1 already exists')

        if LayerDum2Exits==0:
            try:
                Diag = np.round(np.sqrt(np.power(Scale,2)*2),decimals=2)
                sql =   'drop table if exists '+ScheMaDum+'.dum2_'+xminymin+';'+\
                        'create table '+ScheMaDum+'.dum2_'+xminymin+' as '+\
                        'select geom, ST_GeneratePoints(st_buffer(geom,-'+str(Diag)+'),1) geom_point from ('+\
                        'select ST_Buffer(ST_Simplify(ST_SetSRID(wkb_geometry,'+epsg+'),'+str(Diag)+'),-'+str(Diag)+') geom '+\
                        'from '+ ScheMaDum+'.'+'dum1_'+xminymin+') a;'+\
                        'CREATE INDEX ON '+ScheMaDum+'.dum2_'+xminymin+' USING GIST (geom_point);'+\
                        'CREATE INDEX ON '+ScheMaDum+'.dum2_'+xminymin+' USING GIST (geom);'
                LaunchPG(sql)
            except Exception as e:
                print(str(xmin)+' | '+str(ymin)+' process with st_pointonsurface... much longer but no crash')
                LaunchPG(sql.replace('ST_GeneratePoints(st_buffer(geom,-'+str(Diag)+'),1)','st_pointonsurface(st_buffer(geom,-'+str(Diag)+'))'))   
        else:
            PrintLog('dum2 already exists')

        CodeText = ','+','.join(['new'+str(Y)+'.'+codeName+' as c'+str(Y) for Y in YearList])
        CropfieldText = ','+','.join(['new'+str(Y)+'.'+cropfieldName+' as cf'+str(Y) for Y in YearList])
        if CountryInCol:
            CountryText = ','+','.join(['new'+str(Y)+'.country as n'+str(Y) for Y in YearList])
        else:
            CountryText = ' '
        #
        # copy GSA of xmin_ymin to avoid crash
        sql = "create temp table mypolygon_"+xminymin+" as "+\
        "SELECT ST_SetSRID(ST_PolygonFromText('POLYGON(("+str(xmin)+" "+str(ymin)+","+str(xmin+ step + Overlap)+" "+str(ymin)+","+str(xmin+ step + Overlap)+" "+str(ymin+ step + Overlap)+","+str(xmin)+" "+str(ymin+ step + Overlap)+","+str(xmin)+" "+str(ymin)+"))'),3035) AS geom_p ;"+\
        "create index on mypolygon_"+xminymin+" using GIST(geom_p) ;"
        for Y in YearList:
            sql += 'create temp table '+NameParc+'_'+str(Y)+'_'+xminymin+' as select cropfield,original_code,geom from '+ScheMa+'.'+NameParc+'_'+str(Y)+SuffixInput+', mypolygon_'+xminymin+' where ST_Intersects(geom,geom_p) ;'
            sql += 'create index on '+NameParc+'_'+str(Y)+'_'+xminymin+' using GIST(geom);'
        GenTxt = 'left join '+NameParc+'_YYYY_'+xminymin+' newYYYY on ST_Contains(newYYYY.geom,agg.geom_point)'
        JoinText = ' '.join([GenTxt.replace('YYYY',str(Y)) for Y in YearList])
        JoinText=''
        for Y in YearList:
            JoinText+=GenTxt.replace('YYYY',str(Y))+' '

        sql +=  'drop table if exists '+Layer+';'+\
                'create table '+Layer+' as '+\
                'select agg.geom'+\
                CropfieldText+CodeText+CountryText+' '+\
                'from '+ ScheMaDum+'.'+'dum2_'+xminymin+' agg '+\
                JoinText+\
                ' where st_area(agg.geom) > '+str(FilterOut)+';'+\
                CreateAllIndex(Layer)

        LaunchPG(sql)
        LaunchPG('drop table if exists '+ ScheMaDum+'.dum1_'+xminymin+';'+\
                'drop table if exists '+ ScheMaDum+'.dum2_'+xminymin+';')
        return Layer
    except Exception as e:
        print(e)
        return 'error ' + Layer

def RunMultiProc(NumbPool):
    NumbPool = int(NumbPool)
    PrintLog('Loop over the Tiles - run on '+str(NumbPool)+ ' CPUs',taille=40)
    ListAlreadyProc =  GetSQL("SELECT table_name FROM information_schema.tables WHERE table_schema = '"+ScheMaDum+"' AND table_name like 'dum3_%%' ")
    ListAlreadyCheckEmpty =  GetSQL("SELECT table_name FROM information_schema.tables WHERE table_schema = '"+ScheMaDum+"_empty' AND table_name like 'dum3_%%' ")
    TileList2 = []
    for t in TileList:
        if not 'dum3_'+str(int(t[0]))+'_'+str(int(t[1])) in ListAlreadyProc.table_name.to_list():
            if not 'dum3_'+str(int(t[0]))+'_'+str(int(t[1])) in ListAlreadyCheckEmpty.table_name.to_list():
                TileList2.append(t)
    if NumbPool>1:
        AllLayers = Parallel(NumbPool, verbose=5)(delayed(ProcTile)(t) for t in TileList2)
    else:
        AllLayers = [ProcTile(t) for t in TileList2]
    return AllLayers

if True:
    NumbPool = 6
    AllLayers = RunMultiProc(NumbPool)

    if any(s.startswith('error') for s in list(filter(None, AllLayers))):
        NumbPool = 4
        AllLayers = RunMultiProc(NumbPool)

    if any(s.startswith('error') for s in list(filter(None, AllLayers))):
        NumbPool = 1
        AllLayers = RunMultiProc(NumbPool)

    PrintLog('ProcTile done',taille=40)

    if any(s.startswith('error') for s in list(filter(None, AllLayers))):
        for l in AllLayers:
            if l.startswith('error'):
                print(l)
        STILL_SOME_ERRORS


ListAlreadyProc =  GetSQL("SELECT table_name FROM information_schema.tables WHERE table_schema = '"+ScheMaDum+"' AND table_name like 'dum3_%%' ").table_name.to_list()

if True:
    textcol = ','.join(['c'+str(y) for y in YearList]+['cf'+str(y) for y in YearList]+['geom'])
    PrintLog('Merge Tiled Layers',taille=40)
    LaunchPG('drop table if exists '+ScheMaDum+'.dum4;'+\
                'create table '+ScheMaDum+'.dum4 as '+\
                ' UNION '.join(['select '+textcol+' from '+ScheMaDum+'.'+Layer for Layer in ListAlreadyProc] )+';'+ \
                CreateAllIndex(ScheMaDum+'.dum4'))


PrintLog('st_union',taille=40)

AllColumns  = ','.join(['cf'+str(Y) for Y in YearList])+\
              ','+','.join(['c'+str(Y) for Y in YearList])
if CountryInCol:
    AllColumns  += ','+','.join(['n' +str(Y) for Y in YearList])

LaunchPG('drop table if exists '+LayerOut+';'+ \
                'create table '+LayerOut+' as '+ \
                ' select st_union(geom) geom,'+ \
                AllColumns+' '+  \
                'FROM '+ScheMaDum+'.dum4 '+  \
                'GROUP BY '+AllColumns+';'+ \
                CreateAllIndex(LayerOut))
LaunchPG('drop table if exists '+ScheMaDum+'.dum4')

LaunchPG('ALTER table '+LayerOut+' ADD COLUMN cropfield SERIAL PRIMARY KEY;'+\
        'CREATE INDEX ON '+ LayerOut+' (cropfield);'+\
        'ALTER TABLE '+ LayerOut+' ADD COLUMN area_ha REAL;'+\
        'UPDATE '+ LayerOut+' SET area_ha = ST_Area(geom)/10000;'+\
        'CREATE INDEX ON '+ LayerOut+' (area_ha);')


# below is to plot on screen the stack layer and check if all tiles were processed
if True:
    import matplotlib.pyplot as plt

    df = GetSQL('select round(st_x(st_centroid(geom))) x, round(st_y(st_centroid(geom))) y from '+ LayerOut )

    coord = np.round(df/1000).groupby(['x','y']).mean().reset_index()
    coord *= 1000
    fig, ax = plt.subplots()
    ax.plot(coord.x.to_numpy(),coord.y.to_numpy(),'.')

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

    print('Processing Grid resolution = '+str(round(step/1000))+'km')
    print('close the map window and reply to the question: Is the map complete?')
    plt.show()
    inp = input('Is the map complete? Y or N\n')
else:
    inp = 'y'

if inp.lower()[0]=='y':
    LaunchPG( 'DROP SCHEMA IF EXISTS '+ScheMaDum+' CASCADE;')
    LaunchPG( 'DROP SCHEMA IF EXISTS '+ScheMaDum+'_empty CASCADE;')
# LaunchPG('DROP SCHEMA IF  EXISTS '+ScheMaDum)
