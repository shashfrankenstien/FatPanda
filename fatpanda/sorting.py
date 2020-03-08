


class _SortOrder(object):
    __slots__ = ['column', 'order', 'ascending']

    def __init__(self, column, ascending=True):
        self.column = column
        self.ascending = ascending
        self.order = "ASC" if ascending else "DESC"

    @property
    def definition(self):
        return f"{self.column} {self.order}"