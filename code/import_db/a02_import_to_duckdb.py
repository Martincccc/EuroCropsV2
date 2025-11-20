import tempfile
tempfile.tempdir = '/scratch/iordamo/'

import os
import duckdb
import glob
import time

def upload_to_duckdb(conn, region_list, year_list, datadir):

    files_to_upload = []
    print(glob.glob(datadir + '**.parquet'))
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
            print(f"Adding file to upload: {filename}")
            files_to_upload.append(filename)

    tablenames = [x.split('/')[-1].split('.')[0] for x in files_to_upload]
    print(tablenames)

    with open('./code/import_db/02_datapreparation_base_upload.sql', "r") as f:
        sql_script = f.read()
        for i in range(0, len(files_to_upload)):
            print(files_to_upload[i])

            sql = sql_script.format(
                 path_to_baselayer=files_to_upload[i],
                 baselayer_name=tablenames[i],
                 baselayer_gomcol='geometry')
            #print(sql)
            conn.execute(sql)
            

def upload(conf):
    os.environ["DUCKDB_EXTENSION_PATH"] = conf.paths['duckdbextpath']
    conn = duckdb.connect(conf.paths['duckdbpath'], config={"allow_unsigned_extensions": "true", "extension_directory": os.environ["DUCKDB_EXTENSION_PATH"]})
    conn.load_extension('parquet')
    conn.execute("SET progress_bar_time = true;")
    conn.execute("SET enable_geoparquet_conversion = false;")
    conn.load_extension('spatial')

    region_list =conf.region_list
    year_list   =conf.year_list


    t1=time.time()
    print("Importing files to DuckDB")
    upload_to_duckdb(conn, region_list, year_list, conf.paths['fastio_dir'])
    t2=time.time()
    print('-----> exec time = %.2fmn'%((t2-t1)/60))

    conn.close()