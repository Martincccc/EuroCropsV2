import tempfile
tempfile.tempdir = '/scratch/iordamo/'

import json
import download_from_ftp
import import_to_duckdb
import import_to_pgdb

def main(config):
    print("Downloading files from FTP")
    #download_from_ftp.download(config)
    print("Importing files to DuckDB")
    #import_to_duckdb.upload(config)
    print("Importing files to PostgreSQL/PostGIS")
    import_to_pgdb.upload(config)
    

if __name__ == "__main__":

    # read config file 
    with open("conf.json") as json_data_file:
        config = json.load(json_data_file)

    main(config)
config