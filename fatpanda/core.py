import pandas as pd
import sqlite3
from .storeBase import storeBase, dict_factory
from fatpanda import SQLITE_NAME


class _Mask(object):
    __slots__ = ['condition']
    def __init__(self, condition):
        self.condition = condition

class _VirtualSeries(object):
    __slots__ = ['definition']
    def __init__(self, lh=None, rh=None, operator=None):
        if lh is not None and rh is not None and operator is not None:
            lh_col = self._get_col_for_type(lh)
            rh_col = self._get_col_for_type(rh)
            self.definition = f"({lh_col} {operator} {rh_col})"

    def __str__(self):
        return f"VIRTUAL{self.definition}"

    def _get_col_for_type(self, obj):
        if isinstance(obj, _Series):
            return obj.raw_name
        elif isinstance(obj, (int,float)):
            return str(obj)
        if isinstance(obj, _VirtualSeries):
            return obj.definition
        else:
            raise TypeError("Arithmetic Operation not implemented for the type")

    def __another_arith_operator(self, other, operator):
        other_col = self._get_col_for_type(other)
        blank_arith = _VirtualSeries()
        blank_arith.definition = f"({self.definition} {operator} {other_col})"
        return blank_arith

    def __add__(self, other):
        return self.__another_arith_operator(other, "+")

    def __sub__(self, other):
        return self.__another_arith_operator(other, "-")

    def __mul__(self, other):
        return self.__another_arith_operator(other, "*")

    def __div__(self, other):
        return self.__another_arith_operator(other, "/")

    def __mod__(self, other):
        return self.__another_arith_operator(other, "%")


class _Query(storeBase):
    def __init__(self, tablename, columns=None, conditions=[], limit=None):
        super().__init__(SQLITE_NAME)
        self.columns = columns
        self.tablename = tablename
        self.conditions = conditions
        self.limit = limit

    def _select_query(self, limit=None):
        limit = limit or self.limit
        where_clause = ''
        limit_stmt = ''
        if self.conditions:
            where_clause = f"WHERE {' AND '.join(self.conditions)}"
        if isinstance(limit, int):
            limit_stmt = f"LIMIT {limit}"
        columns = self.columns or ['*']
        return f"SELECT {','.join(columns)} FROM {self.tablename} {where_clause} {limit_stmt}".strip()


class _Series(_Query):
    def __init__(self, name, tablename, coltypes, virtual=None, conditions=[], limit=None):
        self.name = name
        self.coltypes = coltypes
        self.virtual = virtual
        cols = ['idx', self.expanded_name]
        super().__init__(tablename, columns=cols, conditions=conditions, limit=limit)

    @property
    def raw_name(self):
        if isinstance(self.virtual, _VirtualSeries):
            return self.virtual.definition
        else:
            return self.name

    @property
    def expanded_name(self):
        if isinstance(self.virtual, _VirtualSeries):
            return f"{self.virtual.definition} AS {self.name}"
        else:
            return self.name

    def get_sql(self):
        return self._select_query()

    def _shallow_copy(self):
        return _Series(self.name, self.tablename, self.coltypes, self.virtual, self.conditions, self.limit)

    def head(self, n=5):
       s = self._shallow_copy()
       s.limit = n
       return s

    def read_into_mem(self):
        data = self.execute(self.get_sql(), row_factory=dict_factory)
        if data:
            sdata = {r['idx']: r[self.name] for r in data}
            return pd.Series(sdata, name=self.name)
        else:
            return pd.Series(name=self.name)

    def __str__(self):
        return str(self.read_into_mem())

    def __getitem__(self, key):
        s = self._shallow_copy()
        s.conditions.append(f"idx={key}")
        return s

    def __setitem__(self, key, value):
        raise NotImplementedError


    def __prep_mask(self, value, condition):
        return _Mask(f"{self.name} {condition} {value}")

    def __eq__(self, value):
        return self.__prep_mask(value, "=")

    def __ne__(self, value):
        return self.__prep_mask(value, "!=")

    def __lt__(self, value):
        return self.__prep_mask(value, "<")

    def __le__(self, value):
        return self.__prep_mask(value, "<=")

    def __gt__(self, value):
        return self.__prep_mask(value, ">")

    def __ge__(self, value):
        return self.__prep_mask(value, ">=")


    def __add__(self, other):
        return _VirtualSeries(self, other, "+")

    def __sub__(self, other):
        return _VirtualSeries(self, other, "-")

    def __mul__(self, other):
        return _VirtualSeries(self, other, "*")

    def __div__(self, other):
        return _VirtualSeries(self, other, "/")

    def __mod__(self, other):
        return _VirtualSeries(self, other, "%")





class _DataFrame(_Query):

    def __init__(self, tablename, columns=None, conditions=[], limit=None, coltypes={}, virtual={}):
        super().__init__(tablename, columns=columns, conditions=conditions, limit=limit)
        self.coltypes = coltypes
        self.virtual = virtual
        if not self.coltypes: self.__learn()

    @property
    def name(self): # proxy self.name for self.tablename
        return self.tablename

    def __learn(self):
        if self.columns == None:
            try:
                self.coltypes = self.get_table_info(self.tablename)
                self.columns = list(self.coltypes.keys())
            except sqlite3.OperationalError:
                if self.is_slice():
                    self.__learn_slice()
                else:
                    raise
        else:
            try:
                types = self.get_table_info(self.name)
                self.coltypes = {k:v for k,v in types.items() if k in self.columns}
            except sqlite3.OperationalError:
                if self.is_slice():
                    self.__learn_slice()
                else:
                    raise

    def __learn_slice(self):
        sample = self.execute(self.get_sql(limit=1), row_factory=dict_factory)
        if len(sample) > 0:
            sample = sample[0]
            self.columns = []
            for k,v in sample.items():
                self.coltypes[k] = self.__value_type(v)
                self.columns.append(k)

    def _shallow_copy(self, columns=None):
        coltypes = self.coltypes.copy()
        virtual = self.virtual.copy()
        if isinstance(columns, (list,set)):
            coltypes = {k:self.coltypes[k] for k in columns if k in self.coltypes}
            virtual = {k:self.virtual[k] for k in columns if k in self.virtual}
        return _DataFrame(self.tablename,
            columns=self.columns.copy(),
            conditions=self.conditions.copy(),
            limit=self.limit,
            coltypes=coltypes,
            virtual=virtual
        )


    def is_slice(self):
        return isinstance(self.limit, int)

    def get_sql(self, limit=None):
        return self._select_query(limit)


    def __value_type(self, val):
        if isinstance(val, int):
            return "INTEGER"
        elif isinstance(val, float):
            return "REAL"
        elif isinstance(val, bool):
            return "BOOLEAN"
        else:
            return "TEXT"


    def __getitem__(self, key):
        if isinstance(key, _Mask):
            df = self._shallow_copy()
            df.conditions.append(key.condition)
            return df
        elif isinstance(key, (list,set)):
            if "idx" not in key: key = ['idx'] + list(key)
            for c in key:
                if c not in self.columns:
                    raise KeyError(c)
            df = self._shallow_copy(columns=key)
            return df
        else:
            coltypes = {
                'idx': self.coltypes['idx'],
                key: self.coltypes[key]
            }
            virtual = None
            if key in self.virtual:
                virtual = self.virtual[key]
            return _Series(key, tablename=self.name, coltypes=coltypes, virtual=virtual)


    def __setitem__(self, key, value):
        if isinstance(value, _VirtualSeries):
            if key in self.columns:
                self.columns.remove(key)
            self.virtual[key] = value
            self.columns.append(f"{value.definition} AS {key}")
            self.coltypes[key] = str(value)
        else:
            print(value)
            raise NotImplementedError
        # CREATE TABLE equipments_backup AS SELECT * FROM csv_csv
        # if self.is_slice():
        #     raise Exception("Cannot set values on a slice")
        # if "key" not in self.columns:
            # self.execute(ALTER TABLE table_name ADD definition_name column_definition)


    def head(self, n=5):
        df = self._shallow_copy()
        df.limit = n
        return df

    def read_into_mem(self):
        data = self.execute(self.get_sql(), row_factory=dict_factory)
        if data:
            return pd.DataFrame(data).set_index("idx", drop=True)
        else:
            return pd.DataFrame()

    def __str__(self):
        return str(self.read_into_mem()) + "\n" + str(self.coltypes)