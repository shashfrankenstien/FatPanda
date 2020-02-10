import sqlite3
import os, sys

from fatpanda import fpd_raw_connection

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def db_connection(f, db=None):
    def wrapper(self, *args, **kwargs):

        conn = fpd_raw_connection(self.db_name)
        conn.row_factory = dict_factory
        cursor = conn.cursor()
        def tracer(frame, event, arg):
            if event=='call':
                frame.f_locals['cursor'] = cursor

        # tracer is activated on next call
        sys.setprofile(tracer)
        try:
            f_res = f(self, *args, **kwargs)
            return f_res
        finally:
            # disable tracer and replace with old one
            sys.setprofile(None)
            conn.commit()
            conn.close()
    return wrapper


def get_cursor():
    return eval('cursor')



class storeBase(object):
    def __init__(self, filepath):
        self.db_name = filepath
        con, cur = self.connection()
        try:
            cur.execute('''PRAGMA journal_mode=WAL;''')
            # Initial connect process
            self.create(cur)
        finally:
            con.close()


    def execute(self, sql, args=(), many=False, autocommit=True, row_factory=None):
        conn, cursor = self.connection(row_factory)
        try:
            cursor.execute(sql, args)
            return cursor.fetchall()
        except:
            print(sql)
            raise
        finally:
            if autocommit:
                conn.commit()
            conn.close()


    def connection(self, row_factory=None):
        conn = fpd_raw_connection(self.db_name)
        conn.row_factory = row_factory #dict_factory
        return conn, conn.cursor()


    def destroy(self):
        os.system('rm {}'.format(self.db_name))

    def create(self, *args, **kwargs):
        pass
        # raise StoreBaseClassError('Subclass StoreBase and implement create() method')

    @db_connection
    def checkForTables(self, tables):
        for name in tables:
            tableExists = get_cursor().execute("SELECT count(*) as c FROM sqlite_master WHERE type='table' AND name=?;", (name, )).fetchone()
            if not tableExists['c']:
                return False
        return True

    def get_table_info(self, tablename):
        res = self.execute(f"PRAGMA table_info({tablename})", row_factory=dict_factory)
        if not res: return res
        return {r['name']: r['type'] for r in res}


class StoreBaseClassError(Exception):
    pass

# if __name__ == '__main__':
#     s = storeBase('./test.db')