"""Analysis tools for dejavu Units."""

__all__ = ['COUNT', 'CrossTab', 'SUM', 'sort']

def sort(attrs):
    """Return a 'cmp' function for list.sort() for Units from attrs.
    
    Each item in the attrs sequence should be a str, the name of an
    attribute on the target Units. Optionally, each item may end with
    " ASC" or " DESC" to indicate direction.
    """
    if isinstance(attrs, basestring):
        attrs = [attrs]
    attrs = [(attr.split(" ", 1)[0], attr.endswith(" DESC"))
             for attr in attrs]
    
    def sort_func(x, y):
        for attr, descending in attrs:
            xv = getattr(x, attr)
            if callable(xv):
                xv = xv()
            if xv is None:
                diff = -1
            else:
                yv = getattr(y, attr)
                if callable(yv):
                    yv = yv()
                if yv is None:
                    diff = 1
                else:
                    diff = cmp(xv, yv)
            if descending:
                diff = -diff
            if diff != 0:
                return diff
        return 0
    return sort_func


def _force_function(attr):
    """If attr is callable, return it, else wrap it in a function."""
    if callable(attr):
        return attr
    
    def g(obj):
        return getattr(obj, attr)
    
    return g


def SUM(attribute):
    """sum(attribute) -> create an aggregate function for use with crosstab().
    
    'attribute' can be either the name of an attribute defined for
    all objects in self.source, or a further callable to which each obj
    is passed and evaluated.
    """
    if callable(attribute):
        def aggfunc(obj, current_agg_value):
            a, b = current_agg_value, attribute(obj)
            if a is None:
                return b
            if b is None:
                return a
            return a + b
    else:
        def aggfunc(obj, current_agg_value):
            a, b = current_agg_value, getattr(obj, attribute)
            if a is None:
                return b
            if b is None:
                return a
            return a + b
    return aggfunc


def COUNT(obj, current_agg_value):
    """count -> an aggregate function for use with crosstab()."""
    return (current_agg_value or 0) + 1


class CrossTab(list):
    """Tool to form crosstabs of Unit property values.
    
    Example:
        >>> f = ["a", "b", "cc", "addd", "a4", "6"]
        >>> group = lambda x: x.isalpha()
        >>> pivot = lambda x: x.startswith("a")
        >>> ctab = analysis.CrossTab(f, [group], pivot)
        >>> rows, columns = ctab.results()
        >>> rows
        {(True,): {False: 2, True: 2}, (False,): {False: 1, True: 1}}
        >>> columns
        [False, True]
    """
    
    def __init__(self, source=[], groups=[], pivot=None, aggfunc=COUNT):
        """CrossTab(source, groups, pivot, aggfunc=count)
        
        source: a list of objects.
        
        groups: a sequence of attribute names or callables,
            which will form the rows of the result.
        
        pivot: either an attribute name or a callable, which will
            form the columns of the result.
        """
        # Iterate through generator if provided. We do this here rather
        # than results() because we want to allow multiple calls to
        # results() without exhausting the generator.
        self.source = [x for x in source]
        
        if not isinstance(groups, (tuple, list)):
            groups = [groups,]
        self.groups = groups
        
        self.pivot = pivot
        self.aggfunc = aggfunc
    
    def results(self):
        # Force all groups to functions. We do it here instead of __init__
        # so consumers can still read self.groups as strings
        # if that's what they supplied.
        groups = [_force_function(group) for group in self.groups]
        pivot = _force_function(self.pivot)
        aggfunc = self.aggfunc
        
        rows = {}
        column_keys = {}
        for obj in self.source:
            key = tuple([group(obj) for group in groups])
            col_key = pivot(obj)
            column_keys[col_key] = None
            
            row = rows.setdefault(key, {})
            row[col_key] = aggfunc(obj, row.get(col_key))
        
        column_keys = column_keys.keys()
        column_keys.sort()
        return rows, column_keys

