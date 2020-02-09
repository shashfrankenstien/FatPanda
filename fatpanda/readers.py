import pandas as pd
import sqlite3
import os
import re
from .core import _DataFrame
from fatpanda import SQLITE_NAME

def splitup(l, n):
	for i in range(0,len(l),n):
		yield l[i:i+n]


def _prep_tablename(name):
    # Remove all non-word characters (everything except numbers and letters)
    name = re.sub(r"[^\w\s]", '', name.replace(".", "_"))
    # Replace all runs of whitespace with a single dash
    name = re.sub(r"\s+", '_', name)
    return name.strip() # Just in case

def _table_loader(chunk, conn, tablename, if_exists):
    chunk.dropna(inplace=True, how="all")
    chunk.reset_index(inplace=True)
    chunk.rename(columns={'index': 'idx'}, inplace=True)
    chunk.to_sql(tablename, conn, if_exists=if_exists, index=False)

def read_csv(filepath, sep=",", header=0, chunksize=1000):
    tablename = _prep_tablename(os.path.split(filepath)[1])
    reader = pd.read_csv(filepath, sep=sep, header=header, chunksize=chunksize)
    if_exists = 'replace'
    conn = sqlite3.connect(SQLITE_NAME)
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
        df_name = f"{_prep_tablename(df_name)}_{len(csv_paths)}_CONCAT"

    if_exists = 'replace'
    conn = sqlite3.connect(SQLITE_NAME)
    for c in csv_paths:
        reader = pd.read_csv(c, sep=sep, header=header, chunksize=chunksize)
        for chunk in reader:
            _table_loader(chunk, conn, df_name, if_exists)
            if_exists = 'append'
    conn.close()
    return _DataFrame(df_name)
