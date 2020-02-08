import pandas as pd
import sqlite3
from .storeBase import storeBase, dict_factory
from fatpanda import SQLITE_NAME


class _Mask(object):
    __slots__ = ['condition']
    def __init__(self, condition):
        self.condition = condition

class _ArithOperation(object):
    __slots__ = ['new_column']
    def __init__(self, new_column):
        self.new_column = new_column


class _Series(storeBase):
    def __init__(self, name, table, query):
        super().__init__(SQLITE_NAME)
        self.name = name
        self.table = table
        self.query = query

    def get_sql(self):
        return self.query

    def head(self, n=5):
       return _Series(self.name, self.table, f"{self.get_sql()} LIMIT {n}")

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
        return _Series(self.name, self.table, f"{self.get_sql()} WHERE idx={key}")

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

    def __prep_arith_operator(self, other, operator):
        if isinstance(other, _Series):
            return _ArithOperation(f"{self.name} {operator} {other.name}")

    def __add__(self, other):
        return self.__prep_arith_operator(other, "+")

    def __sub__(self, other):
        return self.__prep_arith_operator(other, "-")

    def __mul__(self, other):
        return self.__prep_arith_operator(other, "*")

    def __div__(self, other):
        return self.__prep_arith_operator(other, "/")

    def __mod__(self, other):
        return self.__prep_arith_operator(other, "%")





class _DataFrame(storeBase):

    def __init__(self, tablename, columns=None, conditions=[]):
        super().__init__(SQLITE_NAME)
        self.name = tablename
        self.columns = columns
        self.coltypes = {}
        self.derived = {}
        self.conditions = conditions
        self.__learn()

    def __learn(self):
        if self.columns == None:
            try:
                self.coltypes = self.get_table_info(self.name)
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
        sample = self.execute(f"{self.get_sql()} LIMIT 1", row_factory=dict_factory)
        if len(sample) > 0:
            sample = sample[0]
            self.columns = []
            for k,v in sample.items():
                self.coltypes[k] = self.__value_type(v)
                self.columns.append(k)


    def is_slice(self):
        return self.name.startswith("(") \
            and self.name.endswith(")") \
            and "select" in self.name.lower() \
            and " from " in self.name.lower()

    def get_sql(self):
        if self.conditions:
            where_clause = f"WHERE {' AND '.join(self.conditions)}"
        else:
            where_clause = ''
        columns = self.columns or ['*']
        return f"SELECT {','.join(columns)} FROM {self.name} {where_clause}"


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
            return _DataFrame(self.name, self.columns, [key.condition])
        elif isinstance(key, (list,set)):
            if "idx" not in key: key = ['idx'] + list(key)
            return _DataFrame(self.name, key)
        else:
            if key in self.derived:
                inquery = self.derived[key]
            else:
                inquery = key
            return _Series(key, self.name, f"SELECT idx, {inquery} from {self.name}")

    def __setitem__(self, key, value):
        if isinstance(value, _ArithOperation):
            if key in self.columns:
                self.columns.remove(key)
            self.derived[key] = f"{value.new_column} AS {key}"
            self.columns.append(self.derived[key])
            self.coltypes[key] = f"Derived({value.new_column})"
        else:
            raise NotImplementedError
        # CREATE TABLE equipments_backup AS SELECT * FROM csv_csv
        # if self.is_slice():
        #     raise Exception("Cannot set values on a slice")
        # if "key" not in self.columns:
            # self.execute(ALTER TABLE table_name ADD new_column_name column_definition)


    def head(self, n=5):
        return _DataFrame(f"({self.get_sql()} LIMIT {n})")

    def read_into_mem(self):
        data = self.execute(self.get_sql(), row_factory=dict_factory)
        if data:
            return pd.DataFrame(data).set_index("idx", drop=True)
        else:
            return pd.DataFrame()

    def __str__(self):
        return str(self.read_into_mem()) + "\n" + str(self.coltypes)