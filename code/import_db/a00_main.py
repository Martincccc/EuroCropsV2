# import tempfile
# tempfile.tempdir = '/scratch/clamart/'

# import json

RunDownload  = True
RunImportDuckDB = False
RunImportPGDB = True


# 


from code.utils.tools import global_config

# import pdb;set_trace()

def main(global_config):
    if RunDownload:
        print("Downloading files from FTP")
        from code.import_db.a01_download_from_ftp import download as download_from_ftp
        download_from_ftp(global_config)
    if RunImportDuckDB:
        print("Importing files to DuckDB")
        from code.import_db.a02_import_to_duckdb import upload as import_to_duckdb
        import_to_duckdb.upload(global_config)
    if RunImportPGDB:
        print("Importing files to PostgreSQL/PostGIS")
        from code.import_db.a03_import_to_pgdb import upload as import_to_pgdb
        import_to_pgdb(global_config)
        # run 04_update_mapping_tables.py
        # run 05_Create_GSA_all_view_layers.py
        # run 06_create_gsa_crop_grid_2.py
    

if __name__ == "__main__":
    main(global_config)
