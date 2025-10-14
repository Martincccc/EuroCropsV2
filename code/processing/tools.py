import psycopg2
import sys, os
from sqlalchemy import create_engine
import datetime as dt
import pandas as pd
import warnings
import subprocess
import rasterio
import configparser  # For reading configuration file
from io import StringIO


# ===============================
# Utility Functions
# ===============================

def PrintLog(message, taille=0):
    """
    Print a timestamped log message.
    Parameters:
        message (str): Message to print
        taille (int): Optional padding for formatting
    """
    if taille > 0:
        message = message.ljust(taille)
    print('# ' + dt.datetime.now().strftime("%m/%d %H:%M:%S") + ': ' + message + '    ####')


def MakeNewDir(Path):
    """Create a directory if it does not exist."""
    if not os.path.exists(Path):
        os.makedirs(Path)


def RunSysCommand(Commande):
    """
    Run a system command using subprocess and return its output.
    Accepts either a list of command parts or a full command string.
    """
    if isinstance(Commande, list):
        Commande = ' '.join(Commande)
    p = subprocess.Popen(Commande, stdout=subprocess.PIPE, shell=True)
    (output, err) = p.communicate()
    return output


def OpenWithRasterio(FileIn):
    """
    Open a raster file safely with rasterio and return the first band as a NumPy array.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with rasterio.open(FileIn) as ds:
            data = ds.read(1)
    return data


# ===============================
# Configuration
# ===============================

# Read config.ini dynamically
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), '../../data/config.ini')
config.read(config_path)

# Convert all sections into a nested dictionary
config_dict = {section: dict(config.items(section)) for section in config.sections()}

# Convenience alias for the PostGIS section
postgis_cfg = config_dict.get('postgis', {})


# ===============================
# Database Functions
# ===============================

def GetSQL(sql):
    """
    Execute an SQL query and return the result as a pandas DataFrame.
    """
    engine = create_engine(
        f"postgresql://{postgis_cfg['user']}:{postgis_cfg['password']}@"
        f"{postgis_cfg['host']}:{postgis_cfg['port']}/{postgis_cfg['dbname']}"
    )
    df = pd.read_sql(sql, engine)
    return df


def LaunchPG(Commande):
    """
    Execute a PostgreSQL command that does not return a result (e.g., INSERT, UPDATE).
    """
    conn, cur = InitPG()
    cur.execute(Commande)
    FinishPG(conn, cur)


def PgId():
    """
    Return a PostgreSQL connection string for use with GDAL or other tools.
    """
    return (
        f"\"dbname='{postgis_cfg['dbname']}' port='{postgis_cfg['port']}' "
        f"user='{postgis_cfg['user']}' host='{postgis_cfg['host']}' "
        f"password='{postgis_cfg['password']}'\""
    )


def InitPG():
    """
    Initialize a psycopg2 connection and return (connection, cursor).
    """
    conn = psycopg2.connect(
        dbname=postgis_cfg['dbname'],
        port=postgis_cfg['port'],
        user=postgis_cfg['user'],
        host=postgis_cfg['host'],
        password=postgis_cfg['password']
    )
    cur = conn.cursor()
    return conn, cur


def FinishPG(conn, cur):
    """
    Commit changes and close the PostgreSQL connection.
    """
    cur.close()
    conn.commit()
    conn.close()


# def PushDF(df, table_name):
#     """
#     Push a pandas DataFrame to a PostgreSQL/PostGIS table using SQLAlchemy.
#     Automatically converts column names to lowercase.
#     """
#     df.set_axis([x.lower() if isinstance(x, str) else x for x in df.columns], axis=1)
    
#     # Split schema and table if provided as schema.table
#     if '.' in table_name:
#         Schema, table_name = table_name.split('.')
#     else:
#         Schema = None

#     engine = create_engine(
#         f"postgresql://{postgis_cfg['user']}:{postgis_cfg['password']}@"
#         f"{postgis_cfg['host']}:{postgis_cfg['port']}/{postgis_cfg['dbname']}"
#     )

#     df.to_sql(table_name, engine, schema=Schema, if_exists='replace', index=False)

# def copy_from_stringio(conn, df, table):
#     """
#     Here we are going save the dataframe in memory 
#     and use copy_from() to copy it to the table
#     """
#     # save dataframe to an in memory buffer
#     buffer = StringIO()
#     df.to_csv(buffer, index_label='id', header=False)
#     buffer.seek(0)
    
#     cursor = conn.cursor()
#     try:
#         cursor.copy_from(buffer, table, sep=",")
#         conn.commit()
#     except (Exception, psycopg2.DatabaseError) as error:   
#         print("Error: %s" % error)
#         conn.rollback()
#         cursor.close()
#         return 1
#     cursor.close()

# def PushDF3(df,table_name,Init=True,Index=[]):
#     TN1 = table_name.lower()
#     TN2 = TN1.replace('.','_')
#     if Init :
#         LaunchPG('drop table if exists '+TN1)
#         LaunchPG('drop table if exists '+TN2)
#         LaunchPG('drop table if exists '+TN1.split('.')[0]+'.'+TN2)
#         PushDF(df.head(1),TN2) # initialisation / include columns names / very slow if all table
#         df = df.iloc[1:]

#     conn = connect(postgis_cfg) # connect to the database
#     copy_from_stringio(conn, df,TN2) # copy the dataframe to SQL / much faster
#     conn.close() # close the connection
#     LaunchPG('ALTER TABLE public.'+TN2+' SET SCHEMA '+TN1.split('.')[0]+';')
#     # print('ALTER TABLE '+TN1.split('.')[0]+'.'+TN2+' RENAME TO '+TN1.split('.')[1]+';')
#     LaunchPG('ALTER TABLE '+TN1.split('.')[0]+'.'+TN2+' RENAME TO '+TN1.split('.')[1]+';')
#     for ind in Index:
#         LaunchPG('CREATE INDEX ON ' +TN1+' ('+ind+');')


# def connect(params_dic):
#     """ Connect to the PostgreSQL database server """
#     conn = psycopg2.connect(**postgis_cfg)
#     return conn



import pandas as pd
import psycopg2
from io import StringIO
from sqlalchemy import create_engine, text



def create_pg_engine(cfg):
    """Create a SQLAlchemy engine from a config dictionary."""
    return create_engine(
        f"postgresql://{cfg['user']}:{cfg['password']}@"
        f"{cfg['host']}:{cfg['port']}/{cfg['dbname']}"
    )

def connect_pg(cfg):
    """Create a psycopg2 connection from a config dictionary."""
    return psycopg2.connect(
        dbname=cfg['dbname'],
        user=cfg['user'],
        password=cfg['password'],
        host=cfg['host'],
        port=cfg['port']
    )



from sqlalchemy import create_engine, text

engine = create_pg_engine(postgis_cfg)

def to_sql_with_indexes(df, table_name, engine=engine, if_exists="replace", index_cols=None):
    """
    Write a DataFrame to PostgreSQL using pandas.to_sql(),
    and optionally create indexes on specified columns.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to write to PostgreSQL.
    table_name : str
        Name of the table, optionally including schema (e.g. "public.my_table").
    engine : sqlalchemy.Engine
        SQLAlchemy engine connected to PostgreSQL.
    if_exists : str, optional
        'fail', 'replace', or 'append' (default 'replace').
    index_cols : list or str, optional
        - If list, create indexes on those columns.
        - If 'all', create indexes on all columns.
        - If None, no indexes are created.
    """
    # --- Parse schema and table ---
    if '.' in table_name:
        schema, table_name = table_name.split('.', 1)
    else:
        schema = 'public'

    # --- Normalize column names ---
    df.columns = [str(c).strip().lower() for c in df.columns]

    # --- Write DataFrame to Postgres ---
    df.to_sql(table_name, engine, schema=schema, if_exists=if_exists, index=False)

    # --- Handle which columns to index ---
    if index_cols == 'all':
        index_cols = list(df.columns)
    elif not index_cols:
        index_cols = []

    # --- Create indexes ---
    with engine.begin() as conn:
        for col in index_cols:
            if col not in df.columns:
                print(f"Skipping index on '{col}' (not in DataFrame columns)")
                continue
            idx_name = f"idx_{table_name}_{col}"
            sql = text(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{schema}"."{table_name}" ("{col}");')
            conn.execute(sql)

    print(f"✅ Table {schema}.{table_name} created with indexes on {index_cols or 'no columns'}.")




from io import StringIO
import psycopg2
import pandas as pd
import csv

def copy_from_stringio(conn, df: pd.DataFrame, full_table_name: str):
    """
    Fast copy of a DataFrame to Postgres using COPY FROM STDIN via StringIO.
    Handles special characters and commas safely.
    """
    # Create an in-memory text buffer
    buffer = StringIO()

    # Use quoting to handle commas/special characters safely
    df.to_csv(
        buffer,
        index=False,
        header=False,
        sep=",",
        quoting=csv.QUOTE_MINIMAL,   # <-- ensures fields with commas or quotes are handled
        escapechar="\\",              # <-- escape quotes properly
        na_rep='\\N'                  # <-- represent NULLs for PostgreSQL
    )
    buffer.seek(0)

    with conn.cursor() as cursor:
        try:
            cursor.copy_expert(f"COPY {full_table_name} FROM STDIN WITH CSV ESCAPE '\\' NULL '\\N';", buffer)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"Error copying data to {full_table_name}: {e}")

def push_df(df: pd.DataFrame, table_name: str, cfg=postgis_cfg, init: bool = True, indexes: list[str] = None):
    """
    Push a pandas DataFrame to PostgreSQL efficiently.
    
    Steps:
    1. Optionally drops & recreates table.
    2. Initializes structure using SQLAlchemy (only for the first row).
    3. Loads the rest of the DataFrame with psycopg2 COPY (very fast).
    4. Moves the table to target schema and renames it back.
    5. Optionally creates indexes.
    """
    if indexes is None:
        indexes = []

    # Normalize table and schema
    table_name = table_name.lower()
    if '.' in table_name:
        schema, table = table_name.split('.')
    else:
        schema, table = 'public', table_name

    temp_table = f"{schema}.{table}_tmp"

    # Convert column names to lowercase
    df.columns = [str(c).lower() for c in df.columns]

    engine = create_pg_engine(cfg)

    if init:
        # Drop potential remnants of old tables
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{table} CASCADE"))
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table} CASCADE"))

        # Initialize table structure by writing only the first row
        df.head(0).to_sql(table + "_tmp", engine, schema=schema, if_exists="replace", index=False)

    # Bulk insert with COPY FROM
    conn = connect_pg(cfg)
    try:
        copy_from_stringio(conn, df, temp_table)
    finally:
        conn.close()

    # Rename temporary table to final name
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {temp_table} RENAME TO {table};"))

        # Recreate indexes if specified
        for ind in indexes:
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table}_{ind} ON {schema}.{table} ({ind});"))

    print(f"✅ DataFrame successfully pushed to {schema}.{table}")

