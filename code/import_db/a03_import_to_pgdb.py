import tempfile
tempfile.tempdir = '/scratch/iordamo/'

import os
import glob
import psycopg2
import pyarrow.parquet as pq
import geopandas as gpd
from sqlalchemy import create_engine
from geoalchemy2 import Geometry
from shapely import wkb 
import pandas as pd
from code.utils.tools import to_sql_with_indexes

def upload_to_postgis(cur, conn, engine, datadir, region_list, year_list, postgis_schema, stack_flag):
    # Create schema if it doesn't exist
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {postgis_schema};")
    conn.commit()

    files_to_upload = []
    for filename in glob.iglob(datadir + '**.parquet'):

        if region_list==['all']:
            nutsok = True
        elif filename.split('/')[-1].split('.')[0].split('_')[0] in region_list:
            nutsok = True
        else:
            nutsok = False
        if year_list==['all']:
            yearok = True
        elif filename.split('/')[-1].split('.')[0].split('_')[1] in [str(y) for y in year_list]:
            yearok = True
        else:
            yearok = False

        if nutsok and yearok:
            files_to_upload.append(filename)

        if stack_flag and region_list==['all'] and filename.split('/')[-1].split('.')[0].split('_')[1] == 'stack':
            stackok = True
        elif stack_flag and filename.split('/')[-1].split('.')[0].split('_')[0] in region_list and filename.split('/')[-1].split('.')[0].split('_')[1] == 'stack':
            stackok = True
        else:
            stackok = False
        if stackok:
            print(f"Adding file to upload: {filename}")
            files_to_upload.append(filename)
    #print(files_to_upload)

    tablenames = [x.split('/')[-1].split('.')[0] for x in files_to_upload]
    print(tablenames)

    for i in range(0, len(files_to_upload)):
        print(files_to_upload[i])
        cur.execute(f"DROP TABLE IF EXISTS {tablenames[i]};")

        #open parquet file with pyarrow
        parquet_file = pq.ParquetFile(files_to_upload[i])

        for ii in range(parquet_file.num_row_groups):
            #read one row group at a time
            arrow_table = parquet_file.read_row_group(ii)

            #convert to pandas/GeoDataFrame
            df = arrow_table.to_pandas()
            df['geometry'] = gpd.GeoSeries.from_wkb(df['geometry'])
            gdf = gpd.GeoDataFrame(df, geometry="geometry")

            #ensure CRS is set
            if gdf.crs is None:
                gdf.set_crs("EPSG:3035", inplace=True)

            #append to PostGIS
            gdf.to_postgis(
                name=tablenames[i],
                con=engine,
                schema=postgis_schema,
                if_exists="append" if ii > 0 else "replace",
                index=False
            )
        
        cur.execute(f"ALTER TABLE {postgis_schema}.{tablenames[i]} RENAME COLUMN geometry TO geom;")

        print("Upload complete")

        cur.execute(f"""CREATE INDEX IF NOT EXISTS {tablenames[i]}_geom_idx ON {postgis_schema}.{tablenames[i]} USING GIST (geom);""")
        cur.execute(f"""CREATE INDEX IF NOT EXISTS {tablenames[i]}_cropfield_idx ON {postgis_schema}.{tablenames[i]} (cropfield);""")
        cur.execute(f"""CREATE INDEX IF NOT EXISTS {tablenames[i]}_area_ha_idx ON {postgis_schema}.{tablenames[i]} (area_ha);""")

        if filename.split('/')[-1].split('.')[0].split('_')[1] != 'stack':
            cur.execute(f"""CREATE INDEX IF NOT EXISTS {tablenames[i]}_original_code_idx ON {postgis_schema}.{tablenames[i]} (original_code);""")

        conn.commit()


def upload(conf):

    local_dir   =conf.paths['fastio_dir']
    region_list =conf.region_list
    year_list   =conf.year_list


    engine = create_engine("postgresql+psycopg2://"+conf.postgis['pg_user']+":"+conf.postgis['pg_password']+"@"+conf.postgis['pg_host']+":"+conf.postgis['pg_port']+"/"+conf.postgis['pg_dbname'])

    conn = psycopg2.connect(
        dbname=conf.postgis['pg_dbname'],
        user=conf.postgis['pg_user'],
        password=conf.postgis['pg_password'],
        host=conf.postgis['pg_host'],
        port=conf.postgis['pg_port']
    )


    region_list =conf.region_list
    year_list   =conf.year_list


    try:
        cur = conn.cursor()
        upload_to_postgis(cur, conn, engine, local_dir,region_list,year_list,conf.postgis['pg_gsa_schema'], conf.parameters['stack'])

        df = pd.read_csv('./data/cropcodemapping/eurocrops.csv')
        to_sql_with_indexes(df,conf.postgis['pg_gsa_schema']+'.eurocrops',index_cols='all')

        df = pd.read_csv('./data/cropcodemapping/hcat4_agriprod_mapping.csv')
        to_sql_with_indexes(df,conf.postgis['pg_gsa_schema']+'.hcat4_agriprod_mapping',index_cols='all')

        df = pd.read_csv('./data/cropcodemapping/hcat4.csv')
        to_sql_with_indexes(df,conf.postgis['pg_gsa_schema']+'.hcat4',index_cols='all')

        df = pd.read_csv('./data/cropcodemapping/hcat4_hrlmapping.csv')
        to_sql_with_indexes(df,conf.postgis['pg_gsa_schema']+'.hcat4_hrlmapping',index_cols='all')

        df = pd.read_csv('./data/cropcodemapping/hcat4_eagle_mapping.csv')
        to_sql_with_indexes(df,conf.postgis['pg_gsa_schema']+'.hcat4_eagle_mapping',index_cols='all')

        df = pd.read_csv('./data/cropcodemapping/agriprod_fadn_mapping.csv')
        to_sql_with_indexes(df,conf.postgis['pg_gsa_schema']+'.agriprod_fadn_mapping',index_cols='all')

    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()