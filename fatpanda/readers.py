import pandas as pd
import sqlite3
import os
import re
import random, string
import hashlib

from .core import _DataFrame
from fatpanda import fpd_raw_connection, SQLITE_NAME


def salt(n):
    return ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(n)])

def source_hasher(source):
    return hashlib.sha256(source.encode()).hexdigest()


def splitup(l, n):
	for i in range(0,len(l),n):
		yield l[i:i+n]


def _prep_tablename(name):
    # Remove all non-word characters (everything except numbers and letters)
    name = re.sub(r"[^\w\s]", '', name.replace(".", "_"))
    # Replace all runs of whitespace with a single dash
    name = re.sub(r"\s+", '_', name)
    return name.strip() # Just in case


def _create_reuse_tablename(source, conn, fallback_name=None):
    srchash = source_hasher(source)
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM __fpd_sys_tables WHERE source_hash = ?", (srchash,))
    name = cur.fetchone()
    if name:
        name = name[0]
    else:
        name = fallback_name
        cur.execute("INSERT INTO __fpd_sys_tables (source_hash, table_name) VALUES (?, ?)", (srchash, name))
    cur.close()
    return name


def _table_loader(chunk, conn, tablename, if_exists):
    chunk.dropna(inplace=True, how="all")
    if "idx" not in chunk:
        chunk.reset_index(inplace=True)
        chunk.rename(columns={'index': 'idx'}, inplace=True)
    chunk.to_sql(tablename, conn, if_exists=if_exists, index=False)


def read_csv(filepath, sep=",", header=0, chunksize=1000):
    tablename = _prep_tablename(os.path.split(filepath)[1])
    reader = pd.read_csv(filepath, sep=sep, header=header, chunksize=chunksize)
    if_exists = 'replace'
    conn = fpd_raw_connection(SQLITE_NAME)
    for chunk in reader:
        _table_loader(chunk, conn, tablename, if_exists)
        if_exists = 'append'
    conn.close()
    return _DataFrame(tablename)


def concat(objs, persist=False):
    pass


def concat_csv(csv_paths, df_name=None, sep=",", header=0, chunksize=1000):
    if not isinstance(csv_paths, (list,set)):
        raise TypeError("Expected iterable csv_paths")
    if df_name is None:
        df_name = sorted([os.path.basename(c) for c in csv_paths])[0]
        df_name = f"{_prep_tablename(df_name)}_{len(csv_paths)}_CONCAT"#_{salt(10)}"

    if_exists = 'replace'
    conn = fpd_raw_connection(SQLITE_NAME)
    df_name = _create_reuse_tablename(''.join(csv_paths), conn, fallback_name=df_name)

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
    df_name = df_name or f"read_sql_query_{salt(10)}"
    df_name = _create_reuse_tablename(query, sqliteconn, fallback_name=df_name)

    if_exists = 'replace'
    for chunk in pd.read_sql_query(query, cnxn, chunksize=100000):
        _table_loader(chunk, sqliteconn, df_name, if_exists)
        if_exists = 'append'
    cnxn.close()
    return _DataFrame(df_name)
