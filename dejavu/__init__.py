"""Dejavu is an Object-Relational Mapper.

Persisted objects are called "Units", and are served into Sandboxes bound
to a StorageManager. Each Unit instance has a class, which maintains its
schema via Unit Properties.

"Dejavu", to quote Flying Circus episode 16, means "that strange feeling
we sometimes get that we've lived through something before." What better
name for a persistence system? Our terminology reflects this cognitive bent:
sandboxes "memorize", "recall" and "forget" Units.

Most Unit lifecycles follow the same pattern:
    aUnit = sandbox.unit(cls, ID=ID)
    val = aUnit.propertyName
    aUnit.propertyName = newValue
    del aUnit # or otherwise release the reference, e.g. close the scope.

When creating new Units, a similar pattern would be:
    newUnit = unit_class()
    newUnit.propertyName = newValue
    sandbox.memorize(newUnit)
    del newUnit # or otherwise release the reference.

Using recall(), you get a list; using xrecall(), you get an iterator:
    for unit in sandbox.recall(cls, expr):
        do_something_with(unit)

You destroy a Unit via Unit.forget().

Applications only need to call Unit.repress() when they wish to stop
caching the object, returning it to storage. This is very rare, and
should really only be performed within dejavu code.
"""

__version__ =  "2.0alpha"


import datetime as _datetime

from geniusql import logic, logicfuncs, _AttributeDocstrings

from dejavu import logflags
from dejavu import analysis
sort = analysis.sort

from dejavu.sandboxes import Sandbox
from dejavu.schemas import *
from dejavu.units import *


class Query(object):
    """A query with relation, attributes, and restriction expressions."""
    
    __metaclass__ = _AttributeDocstrings
    
    relation = None
    relation__doc = """A single Unit class or a UnitJoin (of Unit classes)."""
    
    attributes = None
    attributes__doc = """
    If None (the default), all attributes are returned.
    Otherwise, if the relation is a single Unit class, this value
    must be a sequence of attribute names for that class. If the
    relation is a UnitJoin, this must be a sequence of sequences
    of attribute names. That is:
        [('ID', 'Size', ...),
         ('ID', 'Value', ...),
         ...]
    The order of sequences must match the order of classes given in the
    relation (and therefore the restriction args, if applicable).
    A final option is to pass a lambda (or Expression) which returns
    the attributes as a tuple or list; e.g.:
        lambda x, y: (x.a, x.b - now(), x.c + y.a)
    This allows access to binary operations and builtin functions."""
    
    restriction = None
    restriction__doc = """
    An Expression (or lambda, or dict) to restrict the rows returned.
    
    For SQL backends, this will be used to construct a WHERE clause.
    The args must be in the same order as the classes in the relation."""
    
    def __init__(self, relation, attributes=None, restriction=None):
        self.relation = relation
        
        from types import FunctionType
        if isinstance(attributes, FunctionType):
            attributes = logic.Expression(attributes)
        self.attributes = attributes
        
        if restriction is None:
            restriction = logic.Expression(lambda *args: True)
        elif not isinstance(restriction, logic.Expression):
            restriction = logic.Expression(restriction)
        
        self.restriction = restriction
    
    def __str__(self):
        if isinstance(self.relation, UnitJoin):
            rel = str(self.relation)
        elif self.relation is None:
            rel = None
        else:
            rel = self.relation.__name__
        return "Query(%s, %s, %s)" % (rel, self.attributes, self.restriction)


class Statement(object):
    """A relational statement, including query, order, limit, offset, and distinct.
    
    query: a Query instance, or a tuple of arguments to form a Query.
    
    order: if given, this will be used to order the results of the Query.
        If the relation is a single Unit class, this value may be a sequence
        of property names for the class. If the relation is a Join, this must
        be an Expression (or lambda) which returns a tuple or list of
        attributes; the args must be in the same order as the classes in
        the relation.
    """
    
    def __init__(self, query, order=None, limit=None, offset=None, distinct=None):
        if not isinstance(query, Query):
            query = Query(*query)
        self.query = query
        
        self.order = order
        self.limit = limit
        self.offset = offset
        self.distinct = distinct

