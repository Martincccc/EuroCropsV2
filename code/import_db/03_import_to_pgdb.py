import tempfile
tempfile.tempdir = '/scratch/iordamo/'

import os
import glob
import psycopg2
import pyarrow.parquet as pq
import geopandas as gpd
from sqlalchemy import create_engine
from shapely import wkb 

def upload_to_postgis(cur, conn, engine, datadir, postgis_schema):
    # Create schema if it doesn't exist
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {postgis_schema};")
    conn.commit()

    files_to_upload = []
    for filename in glob.iglob(datadir + '**.parquet'):
        files_to_upload.append(filename)
    #print(files_to_upload)

    tablenames = [x.split('/')[-1].split('.')[0] for x in files_to_upload]
    #print(tablenames)

    for i in range(0, len(files_to_upload)):
        print(files_to_upload[i])

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

            print("Upload complete")

        cur.execute(f"""CREATE INDEX IF NOT EXISTS {tablenames[i]}_geometry_idx ON {postgis_schema}.{tablenames[i]} USING GIST (geometry);""")
        cur.execute(f"""CREATE INDEX IF NOT EXISTS {tablenames[i]}_cropfield_idx ON {postgis_schema}.{tablenames[i]} (cropfield);""")
        cur.execute(f"""CREATE INDEX IF NOT EXISTS {tablenames[i]}_original_code_idx ON {postgis_schema}.{tablenames[i]} (original_code);""")
        cur.execute(f"""CREATE INDEX IF NOT EXISTS {tablenames[i]}_area_ha_idx ON {postgis_schema}.{tablenames[i]} (area_ha);""")

    conn.commit()


def upload(conf):
    engine = create_engine("postgresql+psycopg2://"+conf['pg_user']+":"+conf['pg_password']+"@"+conf['pg_host']+":"+conf['pg_port']+"/"+conf['pg_dbname'])

    conn = psycopg2.connect(
        dbname=conf['pg_dbname'],
        user=conf['pg_user'],
        password=conf['pg_password'],
        host=conf['pg_host'],
        port=conf['pg_port']
    )

    try:
        cur = conn.cursor()
        upload_to_postgis(cur, conn, engine, conf['gpqt_output_path'], conf['pg_schema'])
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()