import psycopg2
import sys, os
from sqlalchemy import create_engine
import datetime as dt
import pandas as pd
import warnings
import subprocess
import rasterio
import configparser  # For reading configuration file


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


def PushDF(df, table_name):
    """
    Push a pandas DataFrame to a PostgreSQL/PostGIS table using SQLAlchemy.
    Automatically converts column names to lowercase.
    """
    df.set_axis([x.lower() if isinstance(x, str) else x for x in df.columns], axis=1)
    
    # Split schema and table if provided as schema.table
    if '.' in table_name:
        Schema, table_name = table_name.split('.')
    else:
        Schema = None

    engine = create_engine(
        f"postgresql://{postgis_cfg['user']}:{postgis_cfg['password']}@"
        f"{postgis_cfg['host']}:{postgis_cfg['port']}/{postgis_cfg['dbname']}"
    )

    df.to_sql(table_name, engine, schema=Schema, if_exists='replace', index=False)
