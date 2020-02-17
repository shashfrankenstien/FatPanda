import pandas as pd
import sqlite3
import os


from .core import _DataFrame
from .utils import source_hasher
from fatpanda import fpd_raw_connection, SQLITE_NAME


# ------------------------------------------------------------
#                           Helpers
# ------------------------------------------------------------


def _table_loader(chunk, conn, tablename, if_exists):
    chunk.dropna(inplace=True, how="all")
    if "idx" not in chunk:
        chunk.reset_index(inplace=True)
        chunk.rename(columns={'index': 'idx'}, inplace=True)
    chunk.to_sql(tablename, conn, if_exists=if_exists, index=False)



# ------------------------------------------------------------
#                      Exposed features
# ------------------------------------------------------------


def read_csv(filepath, df_name=None, sep=",", header=0, chunksize=1000):
    df_name = df_name or source_hasher(filepath)
    reader = pd.read_csv(filepath, sep=sep, header=header, chunksize=chunksize)
    if_exists = 'replace'
    conn = fpd_raw_connection(SQLITE_NAME)
    for chunk in reader:
        _table_loader(chunk, conn, df_name, if_exists)
        if_exists = 'append'
    conn.close()
    return _DataFrame(df_name)


def concat(objs, persist=False):
    pass


def concat_csv(csv_paths, df_name=None, sep=",", header=0, chunksize=1000):
    if not isinstance(csv_paths, (list,set)):
        raise TypeError("Expected iterable csv_paths")
    df_name = df_name or source_hasher(''.join(csv_paths))

    if_exists = 'replace'
    conn = fpd_raw_connection(SQLITE_NAME)
    # df_name = _create_reuse_tablename(''.join(csv_paths), conn, fallback_name=df_name)
    for c in csv_paths:
        reader = pd.read_csv(c, sep=sep, header=header, chunksize=chunksize)
        for chunk in reader:
            _table_loader(chunk, conn, df_name, if_exists)
            if_exists = 'append'
    conn.close()
    return _DataFrame(df_name)


def read_sql_query(query, cnxn, df_name=None):
    # cnxn = pyodbc.connect('DRIVER={SQL SERVER};server=10.91.141.195;database=Pricing;uid=pricing_admin;pwd=admin')
    sqliteconn = fpd_raw_connection(SQLITE_NAME)
    df_name = df_name or source_hasher(query)

    if_exists = 'replace'
    for chunk in pd.read_sql_query(query, cnxn, chunksize=100000):
        _table_loader(chunk, sqliteconn, df_name, if_exists)
        if_exists = 'append'
    cnxn.close()
    return _DataFrame(df_name)
