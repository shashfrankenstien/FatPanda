import sqlite3, os
SQLITE_NAME = "fatpanda.tmp.db"
# if os.path.isfile(SQLITE_NAME): os.remove(SQLITE_NAME)


def fpd_raw_connection(db_path=SQLITE_NAME):
    conn = sqlite3.connect(db_path)
    '''Optional processing'''
    return conn

from .readers import (
    read_csv,
    concat_csv,
    read_sql_query
)