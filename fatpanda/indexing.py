from functools import reduce

class _Mask(object):
    '''column mask'''
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
        elif isinstance(rhs, (list,set,tuple,slice)):
            raise NotImplementedError
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
        if slc.start is not None:
            masks.append(cls(lhs, slc.start, ">="))

        if slc.stop is not None:
            masks.append(cls(lhs, slc.stop, "<"))

        if slc.step is not None:
            start = slc.start or 0
            mod_val = f"(({lhs}-{start})%{slc.step})"
            masks.append(cls(mod_val, 0, "=="))

        if masks:
            return reduce(lambda m1,m2: m1 & m2, masks)
        else:
            return None



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
