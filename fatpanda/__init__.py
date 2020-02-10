import sqlite3
SQLITE_NAME = "fatpanda.tmp.db"

SYS_TABLES = '''
CREATE TABLE IF NOT EXISTS "__fpd_sys_tables" (
    source_hash TEXT,
    table_name TEXT
)
'''

def fpd_raw_connection(db_path=SQLITE_NAME):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(SYS_TABLES)
    cur.close()
    return conn

from .readers import (
    read_csv,
    concat_csv,
    read_sql_query
)