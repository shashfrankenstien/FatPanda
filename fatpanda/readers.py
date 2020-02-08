import pandas as pd
import sqlite3
import os
from .core import _DataFrame
from fatpanda import SQLITE_NAME

def splitup(l, n):
	for i in range(0,len(l),n):
		yield l[i:i+n]



def read_csv(filepath, chunksize=1000, sep=",", header=0):
    tablename = os.path.split(filepath)[1].replace(".", "_")
    reader = pd.read_csv(filepath, sep=sep, header=header, chunksize=chunksize)
    if_exists = 'replace'
    conn = sqlite3.connect(SQLITE_NAME)

    for chunk in reader:
        chunk.dropna(inplace=True, how="all")
        chunk.reset_index(inplace=True)
        chunk.rename(columns={'index': 'idx'}, inplace=True)
        chunk.to_sql(tablename, conn, if_exists=if_exists, index=False)
        if_exists = 'append'

    return _DataFrame(tablename)
