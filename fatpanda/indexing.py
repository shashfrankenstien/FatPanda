from functools import reduce
import warnings

from fatpanda.sorting import _SortOrder
from fatpanda.utils import PandasDivergenceWarning

class _Mask(object):
    '''column mask'''
    __slots__ = ['lhs', 'rhs', 'operation', 'order_by']
    def __init__(self, lhs, rhs, operation):
        if isinstance(lhs, _Mask):
            self.lhs = lhs.condition
        else:
            self.lhs = lhs

        if isinstance(rhs, str) and rhs.lower()!='null':
            self.rhs = f'"{rhs}"'
        elif isinstance(rhs, _Mask):
            self.rhs = rhs.condition
        elif isinstance(rhs, slice):
            raise TypeError("Use _Mask.from_slice instead")
        elif isinstance(rhs, (list,set,tuple)):
            raise NotImplementedError
        else:
            self.rhs = rhs

        self.operation = operation
        self.order_by = []


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
        return _Mask(mask_false, mask_null, "OR")

    def __eq__(self, value):
        return _Mask(self, value, "=")

    def __ne__(self, value):
        return _Mask(self, value, "!=")

    def __lt__(self, value):
        return _Mask(self, value, "<")

    def __le__(self, value):
        return _Mask(self, value, "<=")

    def __gt__(self, value):
        return _Mask(self, value, ">")

    def __ge__(self, value):
        return _Mask(self, value, ">=")

    @classmethod
    def from_slice(cls, lhs, slc):
        if not isinstance(slc, slice):
            raise TypeError("Not a range")
        if isinstance(lhs, cls):
            lhs = lhs.condition

        masks = []
        order_by = []
        ascending = True
        if slc.step is not None:
            if slc.step == 0:
                raise ValueError("slice step cannot be zero")
            elif slc.step > 0:
                so = _SortOrder(lhs, ascending=True)
            elif slc.step < 0:
                if slc.step < -1: warnings.warn(PandasDivergenceWarning(f"result for step = {slc.step} might diverge from pandas")) # FIXME
                so = _SortOrder(lhs, ascending=False)

            start = slc.start or 0
            mod_val = f"(({lhs}-{start})%{abs(slc.step)})"
            masks.append(cls(mod_val, 0, "=="))
            order_by.append(so)
            ascending = so.ascending

        if slc.start is not None:
            if ascending:
                masks.append(cls(lhs, slc.start, ">="))
            else:
                masks.append(cls(lhs, slc.start, "<="))

        if slc.stop is not None:
            if ascending:
                masks.append(cls(lhs, slc.stop, "<"))
            else:
                masks.append(cls(lhs, slc.stop, ">"))

        if masks:
            M = reduce(lambda m1,m2: m1 & m2, masks)
            M.order_by += order_by
            return M
        else:
            return None # Slice is empty




class _LocIndexer(object):

    def __init__(self, obj):
        self._obj = obj

    def _create_filter(self, key):
        if isinstance(key, _Mask):
            return key
        if isinstance(key, slice):
            return _Mask.from_slice(self._obj._index_col, key)
        else:
            return _Mask(self._obj._index_col, key, "=")

    def __make_loc_object(self, obj):
        obj._loc_object = True
        return obj


    def __getitem__(self, key):
        if not isinstance(key, tuple):
            mask = self._create_filter(key)
            return self.__make_loc_object(self._obj[mask])

        elif len(key)==1:
            mask = self._create_filter(key[0])
            return self.__make_loc_object(self._obj[mask])

        elif len(key)==2:
            mask = self._create_filter(key[0])
            return self.__make_loc_object(self._obj[mask][key[1]])

        else:
            raise KeyError("Illegal use of .loc/.iloc[row_indexer, column_indexer]")
