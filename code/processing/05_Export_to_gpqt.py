import tempfile
tempfile.tempdir = '/scratch/iordamo/'

import os
import psycopg2
import geopandas as gpd
from sqlalchemy import create_engine, text
import shapely

#use fastio from m's script
datadir = '/eos/jeodpp/data/projects/REFOCUS/data/tasks/gsa_export_test_4martin'

# connection parameters
host = "0000"
port = "0000"
dbname = "0000"
user = "0000"
password = "0000"
schemas = ['gsa', 'stack']

#set pg connection
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

for schema_name in schemas:
    #get all table names in that schema
    with engine.connect() as conn:
        tables = conn.execute(text(f"""SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_type = 'BASE TABLE';""")).fetchall()

    table_names = [t[0] for t in tables]
    #print(table_names)

    #get all names already in directory
    datadir_gpqt = os.path.join(datadir, 'data/output/gpqt')
    list_of_files = os.listdir(datadir_gpqt)
    list_of_files = [item.removesuffix('.parquet') for item in list_of_files]
    #print(list_of_files)


    #geometry column name check
    #iterate and export
    for table in table_names:
        if table not in list_of_files:
            print(table)
            with engine.connect() as conn:
                geom_col = conn.execute(text(f"""SELECT f_geometry_column FROM geometry_columns WHERE f_table_schema = '{schema_name}' AND f_table_name = '{table}';""")).fetchone()

            if geom_col:
                geom_col = geom_col[0]

                #select the relevant columns
                with engine.connect() as conn:
                    columns = conn.execute(text(f"""select column_name from information_schema.columns where table_schema = '{schema_name}' and table_name = '{table}';""")).fetchall()
                    columns = [c[0] for c in columns]

                    columns_to_select = ['cropfield', 'original_code', 'off_id', 'off_area', 'area_ha', 'geom']
                    final_columns = [c for c in columns_to_select if c in columns]
                    final_columns_str = ", ".join(final_columns)

                #control for schema 
                if schema_name == 'gsa':
                    sql = f'SELECT {final_columns_str} FROM "{schema_name}"."{table}";'

                if schema_name == 'stack':
                    sql = f'SELECT * FROM "{schema_name}"."{table}";'

                with engine.connect() as conn:
                    gdf = gpd.read_postgis(sql, conn, geom_col=geom_col)

                #ensure CRS is set
                if gdf.crs is None:
                    gdf = gdf.set_crs("EPSG:3035")

                #rename geom column to geometry for duckdb
                gdf = gdf.rename_geometry("geometry")

                #set geometry type fo multipolygon
                print(gdf.geom_type.value_counts())
                gdf["geometry"] = gdf["geometry"].apply(lambda g: g if g is None else g if g.geom_type == "MultiPolygon" else shapely.MultiPolygon([g]))

                out_path = os.path.join(datadir, 'data/output/gpqt', f"{table}.parquet")
                gdf.to_parquet(out_path, index=False, compression='zstd', engine='pyarrow')
                print(f"Exported {table} to {table}.parquet")
            else:
                print(f"Table {table} has no geometry column — skipped.")
        else:
            print(f"File for table {table} already exists — skipped.")
