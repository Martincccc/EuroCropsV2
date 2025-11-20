import tempfile
tempfile.tempdir = '/scratch/iordamo/'

import os
import duckdb
import glob
import time

def upload_to_duckdb(conn, datadir):

    files_to_upload = []
    for filename in glob.iglob(datadir + '**.parquet'):
        files_to_upload.append(filename)

    tablenames = [x.split('/')[-1].split('.')[0] for x in files_to_upload]
    print(tablenames)

    with open('datapreparation_base_upload.sql', "r") as f:
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
    os.environ["DUCKDB_EXTENSION_PATH"] = conf['duckdbextpath']
    conn = duckdb.connect(conf['duckdbpath'], config={"allow_unsigned_extensions": "true", "extension_directory": os.environ["DUCKDB_EXTENSION_PATH"]})
    conn.load_extension('parquet')
    conn.execute("SET progress_bar_time = true;")
    conn.execute("SET enable_geoparquet_conversion = false;")
    conn.load_extension('spatial')

    t1=time.time()
    print("Importing files to DuckDB")
    upload_to_duckdb(conn, conf['gpqt_output_path'])
    t2=time.time()
    print('-----> exec time = %.2fmn'%((t2-t1)/60))

    conn.close()