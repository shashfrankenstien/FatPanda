import pandas as pd
import sqlite3

from .storeBase import storeBase, dict_factory
from .utils import salt
from fatpanda import SQLITE_NAME




class _Query(storeBase):
    def __init__(self, tablename, columns=None, conditions=[], limit=None, set_journal_mode=True):
        super().__init__(SQLITE_NAME, set_journal_mode=set_journal_mode)
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


class _Mask(object):
    __slots__ = ['lhs', 'rhs', 'operation']
    def __init__(self, lhs, rhs, operation):
        if isinstance(lhs, _Mask):
            self.lhs = lhs.condition
        else:
            self.lhs = lhs

        if isinstance(rhs, str) and rhs.lower()!='null':
            self.rhs = f'"{rhs}"'
        elif isinstance(rhs, _Mask):
            self.rhs = rhs.condition
        else:
            self.rhs = rhs

        self.operation = operation

    @property
    def condition(self):
        return f"({self.lhs} {self.operation} {self.rhs})"

    def __and__(self, other):
        return _Mask(self, other, "AND")

    def __or__(self, other):
        return _Mask(self, other, "OR")

    def __invert__(self):
        mask_false = _Mask(self, 0, "=")
        mask_null = _Mask(self, "NULL", "IS")
        return _Mask(mask_false.condition, mask_null, "OR")


class _Series(_Query):
    def __init__(self, name, tablename, coltypes, conditions=[], limit=None):
        self._name = name
        self.coltypes = coltypes
        cols = ['idx', self.expanded_name]
        super().__init__(tablename, columns=cols, conditions=conditions, limit=limit, set_journal_mode=False)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def raw_name(self):
        return self._name

    @property
    def type_name(self):
        return f"SERIES({self.coltypes[self._name]})"

    @property
    def expanded_name(self):
        return self._name


    def get_sql(self):
        return self._select_query()

    def _shallow_copy(self):
        return _Series(self.name, self.tablename, self.coltypes, self.conditions, self.limit)

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


    def __prep_mask(self, value, operation):
        return _Mask(self.name, value, operation)

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


    def _arithmetic_helperator(self, other, operator):
        if isinstance(other, _Series):
            if self.tablename!=other.tablename:
                raise NotImplementedError("Arithmetic operation across DataFrames not supported")
            rh_col = other.raw_name
        elif isinstance(other, (int,float)):
            rh_col = str(other)
        else:
            raise NotImplementedError(f"Not implemented for {type(other)}")

        definition = f"({self.raw_name} {operator} {rh_col})"
        dummy_name = ''
        while dummy_name in self.columns:
            dummy_name = salt(10)
        coltypes = {"idx":self.coltypes['idx'], dummy_name:None}
        # Setting dummy name and coltype to None. These attributes will be reset later
        return _VirtualSeries(dummy_name, definition, self.tablename, coltypes, self.conditions, self.limit)


    def __add__(self, other):
        return self._arithmetic_helperator(other, "+")

    def __sub__(self, other):
        return self._arithmetic_helperator(other, "-")

    def __mul__(self, other):
        return self._arithmetic_helperator(other, "*")

    def __div__(self, other):
        return self._arithmetic_helperator(other, "*1.0/") # Multiply by 1.0 to ensure float result

    def __truediv__(self, other):
        return self.__div__(other)

    def __mod__(self, other):
        return self._arithmetic_helperator(other, "%")


class _VirtualSeries(_Series):

    def __init__(self, name, virtual_definition, tablename, coltypes, conditions=[], limit=None):
        self._virtual_definition = str(virtual_definition)
        if coltypes[name] is None:
            coltypes[name] = self.type_name
        super().__init__(name, tablename, coltypes, conditions, limit)

    def __setattr__(self, name, value):
        if name=="name":
            self.coltypes[value] = self.coltypes[self._name]
            self._name = value
            del self.coltypes[self._name]
        super().__setattr__(name, value)

    @property
    def raw_name(self):
        return self._virtual_definition

    @property
    def type_name(self):
        return f"VIRTUAL({self.raw_name})"

    @property
    def expanded_name(self):
        return f"{self._virtual_definition} AS `{self.name}`"

    def _shallow_copy(self):
        return _VirtualSeries(self.name, self._virtual_definition, self.tablename, self.coltypes, self.conditions, self.limit)

    def __setitem__(self, key, value):
        raise TypeError("Cannot set item on VirtualSeries")



class _DataFrame(_Query):

    def __init__(self, tablename, columns=None, coltypes={}, conditions=[], limit=None, virtual={}):
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
        if isinstance(columns, (list,set)):
            coltypes = {k:self.coltypes[k] for k in columns if k in self.coltypes}
            virtual = {k:self.virtual[k] for k in columns if k in self.virtual}
        else:
            coltypes = self.coltypes.copy()
            virtual = self.virtual.copy()
            columns = self.columns.copy()
        return _DataFrame(self.tablename,
            columns=columns,
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
        elif isinstance(val, str):
            return "TEXT"
        # Following added for __setitem__
        elif isinstance(val, (_Series, _VirtualSeries)):
            return val.type_name
        else:
            raise NotImplementedError(type(val))


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
            return self._shallow_copy(columns=key)
        else:
            coltypes = {
                'idx': self.coltypes['idx'],
                key: self.coltypes[key]
            }
            if key in self.virtual:
                return _VirtualSeries(key,
                    tablename=self.name,
                    coltypes=coltypes,
                    virtual_definition=self.virtual[key],
                    conditions=self.conditions.copy(),
                    limit=self.limit
                )
            else:
                return _Series(key,
                    tablename=self.name,
                    coltypes=coltypes,
                    conditions=self.conditions.copy(),
                    limit=self.limit
                )


    def __setitem__(self, key, value):
        value_type = self.__value_type(value)
        if "VIRTUAL" in value_type:
            value.name = key
        else:
            if value_type in ["INTEGER", "REAL", "BOOL"]:
                definition = value
            elif value_type=="TEXT":
                definition = f'"{value}"'
            elif "SERIES" in value_type:
                definition = value.raw_name
            else:
                print(value)
                raise NotImplementedError
            coltypes = {'idx':self.coltypes['idx'], key:None}
            value = _VirtualSeries(key, tablename=self.name, coltypes=coltypes, virtual_definition=definition)

        if key in self.columns:
            self.columns.remove(key)

        self.columns.append(value.expanded_name)
        self.virtual[key] = value.raw_name
        self.coltypes[key] = value.type_name

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