"""Storage Managers for Dejavu."""

import datetime
try:
    set
except NameError:
    from sets import Set as set
import types

import dejavu
from dejavu import errors, logflags, recur, sandboxes, xray
from dejavu.containers import Graph
from geniusql import logic, astwalk


class StorageManager(object):
    """A Manager base class for storing and retrieving Units.
    
    The base StorageManager class doesn't actually store anything;
    it needs to be subclassed.
    
    ----
    
    Regarding large systems:
    See http://www-db.cs.wisc.edu/cidr/cidr2007/papers/cidr07p15.pdf
    
        "A scale-agnostic programming abstraction must have the notion of
        entity as the boundary of atomicity."
    
    If you want Dejavu to function as a scale-aware lower layer, use Units
    to represent Pat Helland's "entities", and use them as a boundary for
    OLTP/CRUD operations like save and delete. DO NOT write StorageManagers
    which mix multiple entities into a single unit. Likewise, large-scale
    Dejavu apps should not expect atomic operations across/between Units.
    
        "...the scale-agnostic application must manage uncertainty
        itself using workflow [instead of distributed transactions]
        if it needs to reach agreement across multiple entities."
    """
    
    def __init__(self, allOptions={}):
        self.classes = set()
        self.associations = Graph(directed=False)
        
        # TODO: move these somewhere else
        self.engine_functions = {}
        
        self.logflags = logflags.ERROR + logflags.IO
    
    def shutdown(self, conflicts='error'):
        """Shut down all connections to internal storage.
        
        conflicts: see errors.conflict.
        """
        pass
    
    def log(self, message):
        """Default logger (writes to stdout). Feel free to replace."""
        if isinstance(message, unicode):
            print message.encode('utf8')
        else:
            print message
    
    def new_sandbox(self):
        """Return a new sandbox object bound to self."""
        return sandboxes.Sandbox(self)
    
    #                               Schemas                               #
    
    def register(self, cls):
        """Assert that Units of class 'cls' will be handled."""
        try:
            # hack for db SM's
            nodename = self.db.name
        except AttributeError:
            nodename = self.__class__.__name__
        
        if self.logflags & logflags.REGISTER:
            self.log(logflags.REGISTER.message(nodename, cls))
        
        self.classes.add(cls)
        
        for ua in cls._associations.itervalues():
            if getattr(ua, "register", True):
                self.associations.connect(cls, ua.farClass)
    
    def register_all(self, globals):
        """Register each subclass of Unit in the given globals."""
        seen = {}
        for obj in globals.itervalues():
            if isinstance(obj, type) and issubclass(obj, dejavu.Unit):
                self.register(obj)
                seen[obj] = None
        return seen.keys()
    
    def class_by_name(self, classname):
        """Return the class object for the given classname."""
        for cls in self.classes:
            if cls.__name__ == classname:
                return cls
        raise KeyError("No registered class found for '%s'." % classname)
    
    def map(self, classes, conflicts='error'):
        """Map classes to internal storage.
        
        conflicts: see errors.conflict.
        """
        for cls in classes:
            if not self.has_storage(cls):
                if conflicts == 'repair':
                    self.create_storage(cls)
                else:
                    errors.conflict(conflicts,
                                    "%s: no storage found." % cls.__name__)
    
    def map_all(self, conflicts='error'):
        """Map all registered classes to internal storage structures.
        
        Although classes can be mapped one at a time, in production it
        is often more useful to map all classes at application startup.
        Call this method to do so (but register all classes first).
        
        This method is idempotent, but that doesn't mean cheap. Try not
        to call it very often (once at app startup is usually enough).
        
        conflicts: see errors.conflict.
        """
        self.map(self.classes, conflicts=conflicts)
    
    def create_database(self, conflicts='error'):
        """Create internal structures for the entire database.
        
        conflicts: see errors.conflict.
        
        This method will NOT create storage for each class, nor will
        it create any dependent properties or indexes.
        """
        raise NotImplementedError("%s has no create_database method."
                                  % self.__class__)
    
    def has_database(self, conflicts='error'):
        """If storage exists for this database, return True."""
        raise NotImplementedError("%s has no has_database method."
                                  % self.__class__)
    
    def drop_database(self, conflicts='error'):
        """Destroy internal structures for the entire database.
        
        conflicts: see errors.conflict.
        
        This method will also drop storage for each class, including
        all properties and indexes.
        """
        raise NotImplementedError("%s has no drop_database method."
                                  % self.__class__)
    
    def create_storage(self, cls, conflicts='error'):
        """Create internal structures for the given class.
        
        conflicts: see errors.conflict.
        
        This method will also create all dependent properties and indexes.
        """
        raise NotImplementedError("%s has no create_storage method."
                                  % self.__class__)
    
    def has_storage(self, cls):
        """If storage structures exist for the given class, return True."""
        raise NotImplementedError("%s has no has_storage method."
                                  % self.__class__)
    
    def drop_storage(self, cls, conflicts='error'):
        """Destroy internal structures for the given class.
        
        conflicts: see errors.conflict.
        
        This method will also drop all dependent properties and indexes.
        """
        raise NotImplementedError("%s has no drop_storage method."
                                  % self.__class__)
    
    def add_property(self, cls, name, conflicts='error'):
        """Add internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        raise NotImplementedError("%s has no add_property method."
                                  % self.__class__)
    
    def has_property(self, cls, name):
        """If storage structures exist for the given property, return True."""
        raise NotImplementedError("%s has no has_property method."
                                  % self.__class__)
    
    def drop_property(self, cls, name, conflicts='error'):
        """Destroy internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        raise NotImplementedError("%s has no drop_property method."
                                  % self.__class__)
    
    def rename_property(self, cls, oldname, newname, conflicts='error'):
        """Rename internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        raise NotImplementedError("%s has no rename_property method."
                                  % self.__class__)
    
    def add_index(self, cls, name, conflicts='error'):
        """Add an index to the given property.
        
        conflicts: see errors.conflict.
        """
        raise NotImplementedError("%s has no add_index method."
                                  % self.__class__)
    
    def has_index(self, cls, name):
        """If an index exists for the given property, return True."""
        raise NotImplementedError("%s has no has_index method."
                                  % self.__class__)
    
    def drop_index(self, cls, name, conflicts='error'):
        """Destroy any index on the given property.
        
        conflicts: see errors.conflict.
        """
        raise NotImplementedError("%s has no drop_index method."
                                  % self.__class__)
    
    
    #                          Data Manipulation                          #
    
    def reserve(self, unit):
        """Reserve storage space for the Unit.
        
        The store should call unit.cleanse() if it saves the whole unit
        state on this call.
        """
        raise NotImplementedError
    
    def save(self, unit, forceSave=False):
        """Store the unit's property values."""
        raise NotImplementedError
    
    def destroy(self, unit):
        """Delete the unit."""
        raise NotImplementedError
    
    def xrecall(self, classes, expr=None, order=None, limit=None, offset=None):
        """Return an iterable of Units."""
        if limit == 0:
            return
        if offset and not order:
            raise TypeError("Order argument expected when offset is provided.")
        
        if isinstance(classes, dejavu.UnitJoin):
            for unitrow in self._xmultirecall(classes, expr, order=order,
                                              limit=limit, offset=offset):
                yield unitrow
            return
        
        raise NotImplementedError
    
    def recall(self, classes, expr=None, order=None, limit=None, offset=None):
        """Return a sequence of Unit instances which satisfy the expression."""
        return [x for x in self.xrecall(classes, expr, order=order,
                                        limit=limit, offset=offset)]
    
    def unit(self, cls, **kwargs):
        """A single Unit which matches the given kwargs, else None.
        
        The first Unit matching the kwargs is returned; if no Units match,
        None is returned.
        """
        try:
            return self.xrecall(cls, logic.filter(**kwargs), limit=1).next()
        except StopIteration:
            return None
    
    def _sort_func(self, order):
        """Return a function (for use with list.sort) from the given order."""
        if order is None:
            return None
        elif isinstance(order, types.FunctionType):
            order = logic.Expression(order)
            return OrderDeparser(order).sort_func()
        elif isinstance(order, logic.Expression):
            return OrderDeparser(order).sort_func()
        elif isinstance(order, (list, tuple)):
            triples = [(0, attr.split(" ", 1)[0], attr.endswith(" DESC"))
                       for attr in order]
            def sort_func(x, y):
                for index, attr, descending in triples:
                    xval = getattr(x[index], attr)
                    if xval is None:
                        diff = -1
                    else:
                        yval = getattr(y[index], attr)
                        if yval is None:
                            diff = 1
                        else:
                            diff = cmp(xval, yval)
                    if descending:
                        diff = -diff
                    if diff != 0:
                        return diff
                return 0
            return sort_func
        else:
            raise TypeError("The 'order' value %r is not one of the allowed "
                            "types (list, lambda, None, or Expression)." %
                            order)
    
    def _paginate(self, data, order=None, limit=None, offset=None, single=False):
        """Manually apply ORDER, LIMIT, and OFFSET operators to a unit stream.
        
        data: an iterable of sequences of Unit instances. For example,
            [(ThingA(), ThingB()), ...]
        single: if False (the default), yield sequences of units. If True,
            yield single Units.
        
        This is a helper function for those Storage Managers which must
        provide their own implementation of ORDER, LIMIT, and OFFSET
        operators. If possible, faster native backends should be used.
        """
        if order:
            # Collapse data iterable into a list.
            data = list(data)
            data.sort(self._sort_func(order))
            data = iter(data)
        elif offset:
            raise TypeError("Order argument expected when offset is provided.")
        
        try:
            for x in xrange(offset or 0):
                data.next()
            
            if limit is not None:
                for x in xrange(limit):
                    if single:
                        yield data.next()[0]
                    else:
                        yield data.next()
            else:
                for unitrow in data:
                    if single:
                        yield unitrow[0]
                    else:
                        yield unitrow
        except StopIteration:
            return
    
    #                                Views                                #
    #
    # The _combine, view, xview, and _multirecall method given below use
    # self.recall to retrieve entire Units, and then slice and dice them
    # in memory. This is quite slow; you should *definitely* override
    # these with provider-specific methods where possible. They are given
    # here as a fallback mechanism only.
    
    def _combine(self, unitjoin, filters):
        """Return (flat) rows of Unit objects for the given (recursive) join."""
        cls1, cls2 = unitjoin.class1, unitjoin.class2
        
        if isinstance(cls1, dejavu.UnitJoin):
            table1 = self._combine(cls1, filters)
            classlist1 = iter(cls1)
        else:
            table1 = [[x] for x in self.recall(cls1, filters[cls1])]
            classlist1 = [cls1]
        
        if isinstance(cls2, dejavu.UnitJoin):
            table2 = self._combine(cls2, filters)
            classlist2 = iter(cls2)
        else:
            table2 = [[x] for x in self.recall(cls2, filters[cls2])]
            classlist2 = [cls2]
        
        # Find an association between the two halves.
        ua = None
        for indexA, clsA in enumerate(classlist1):
            for indexB, clsB in enumerate(classlist2):
                path = unitjoin.path or clsB.__name__
                ua = clsA._associations.get(path, None)
                if ua:
                    nearKey, farKey = ua.nearKey, ua.farKey
                    break
                path = unitjoin.path or clsA.__name__
                ua = clsB._associations.get(path, None)
                if ua:
                    nearKey, farKey = ua.farKey, ua.nearKey
                    break
            if ua: break
        if ua is None:
            msg = ("No association found between %s and %s." % (cls1, cls2))
            raise errors.AssociationError(msg)
        
        # Yield rows of Unit instances
        if unitjoin.leftbiased is None:
            # INNER JOIN
            # Flatten the inner generator to iterate over it multiple times.
            table2 = list(table2)
            for row1 in table1:
                nearVal = getattr(row1[indexA], nearKey)
                for row2 in table2:
                    # Test against join constraint
                    farVal = getattr(row2[indexB], farKey)
                    if nearVal == farVal:
                        yield row1 + row2
        elif unitjoin.leftbiased is True:
            # LEFT JOIN
            # Flatten the inner generator to iterate over it multiple times.
            table2 = list(table2)
            for row1 in table1:
                nearVal = getattr(row1[indexA], nearKey)
                found = False
                for row2 in table2:
                    # Test against join constraint
                    farVal = getattr(row2[indexB], farKey)
                    if nearVal == farVal:
                        yield row1 + row2
                        found = True
                if not found:
                    # Yield dummy objects for table2
                    yield row1 + [unit.__class__() for unit in row2]
        else:
            # RIGHT JOIN
            # Flatten the inner generator to iterate over it multiple times.
            table1 = list(table1)
            for row2 in table2:
                unitB = row2[indexB]
                farVal = getattr(unitB, farKey)
                found = False
                for row1 in table1:
                    # Test against join constraint
                    nearVal = getattr(row1[indexA], nearKey)
                    if nearVal == farVal:
                        yield row1 + row2
                        found = True
                if not found:
                    # Yield dummy objects for table1
                    yield [unit.__class__() for unit in row1] + row2
    
    def _xmultirecall(self, classes, expr=None, order=None, limit=None, offset=None):
        """Yield lists of units of the given classes which match expr."""
        if not isinstance(expr, logic.Expression):
            expr = logic.Expression(expr)
        
        # TODO: deconstruct expr into a set of subexpr's, one for
        # each class in classes.
        filters = dict([(cls, None) for cls in classes])
        
        def _combine_inner():
            for unitrow in self._combine(classes, filters):
                if expr(*unitrow):
                    yield unitrow
        return self._paginate(_combine_inner(), order, limit, offset)
    
    def _multirecall(self, classes, expr=None, order=None, limit=None, offset=None):
        """Return lists of units which satisfy the expression."""
        return [t for t in self._xmultirecall(classes, expr=expr, order=order,
                                              limit=limit, offset=offset)]
    
    def xview(self, query, order=None, limit=None, offset=None, distinct=False):
        """Yield property tuples for the given query."""
        if limit == 0:
            return
        if offset and not order:
            raise TypeError("Order argument expected when offset is provided.")
        
        if not isinstance(query, dejavu.Query):
            query = dejavu.Query(*query)
        
        if self.logflags & logflags.VIEW:
            self.log(logflags.VIEW.message(query, distinct))
        
        if isinstance(query.attributes, logic.Expression):
            attr_is_expr = True
        elif query.attributes is None:
            # Return all attributes (what sort order?)
            raise NotImplementedError("Attribute order is undefined.")
        else:
            attr_is_expr = False
        
        expr = query.restriction
        
        seen = {}
        
        if isinstance(query.relation, dejavu.UnitJoin):
            # TODO: deconstruct expr into a set of subexpr's, one for
            # each class in classes.
            filters = dict([(cls, None) for cls in query.relation])
            data = self._combine(query.relation, filters)
            if order:
                data = [unit for unit in data]
                data.sort(dejavu.sort(order))
                data = iter(data)
            
            def puller():
                for unitrow in data:
                    if expr is None or expr(*unitrow):
                        if attr_is_expr:
                            datarow = tuple(query.attributes(*unitrow))
                        else:
                            datarow = []
                            for i, attrs in enumerate(query.attributes):
                                unit = unitrow[i]
                                if attrs is None:
                                    # Return all attributes (TODO: what sort order?)
                                    raise NotImplementedError("Attribute order is undefined.")
                                else:
                                    for attr in attrs:
                                        datarow.append(getattr(unit, attr))
                            datarow = tuple(datarow)
                        if distinct:
                            if datarow not in seen:
                                yield datarow
                                seen[datarow] = None
                        else:
                            yield datarow
        else:
            data = self.recall(query.relation, expr)
            if order:
                data.sort(dejavu.sort(order))
                data = iter(data)
            
            def puller():
                for unit in data:
                    if expr is None or expr(unit):
                        # Use tuples for hashability.
                        if attr_is_expr:
                            datarow = tuple(query.attributes(unit))
                        else:
                            datarow = tuple([getattr(unit, attr)
                                             for attr in query.attributes])
                        if distinct:
                            if datarow not in seen:
                                yield datarow
                                seen[datarow] = None
                        else:
                            yield datarow
        
        ordered_data = puller()
        try:
            for x in xrange(offset or 0):
                ordered_data.next()
            
            if limit:
                for x in xrange(limit):
                    yield ordered_data.next()
            else:
                for unit in ordered_data:
                    yield unit
        except StopIteration:
            return
    
    def view(self, query, order=None, limit=None, offset=None, distinct=False):
        """Return tuples of attribute values for the given query."""
        return [x for x in self.xview(query, order=order, limit=limit,
                                      offset=offset, distinct=distinct)]
    
    def count(self, cls, expr=None):
        """Number of Units of the given cls which match the given expr."""
        if cls.identifiers:
            uniq = cls.identifiers
        else:
            uniq = cls._properties.keys()
        return len(self.view((cls, uniq, expr), distinct=True))
    
    def range(self, cls, attr, expr=None):
        """Distinct, non-None attr values (ordered and continuous, if possible).
        
        If the given attribute is a known discrete, ordered type
        (like int, long, datetime.date), this returns the closed interval:
            
            [min(attr), ..., max(attr)]
        
        That is, all possible values will be output between min and max,
        even if they do not appear in the dataset.
        
        If the given attribute is not reasonably discrete (e.g., str,
        unicode, or float) then all distinct, non-None values are returned
        (sorted, if possible).
        """
        query = dejavu.Query(cls, [attr], expr)
        existing = [x[0] for x in self.xview(query, distinct=True)
                    if x is not None]
        if not existing:
            return []
        
        attr_type = getattr(cls, attr).type
        if issubclass(attr_type, (int, long)):
            return range(min(existing), max(existing) + 1)
        else:
            if issubclass(attr_type, datetime.date):
                def date_gen():
                    start, end = min(existing), max(existing)
                    for d in range((end + 1) - start):
                        yield start + datetime.timedelta(d)
                return date_gen()
        
        try:
            existing.sort()
        except TypeError:
            pass
        
        return existing
    
    def sum(self, cls, attr, expr=None):
        """Sum of all non-None values for the given cls.attr."""
        expr = logic.Expression(lambda x: getattr(x, attr) != None) + expr
        return sum([row[0] for row in
                    self.xview(dejavu.Query(cls, (attr,), expr))])
    
    #                            Transactions                             #
    
    # By default, stores do not support Transactions.
    # Override these with appropriate methods as you are able.
    start = None
    rollback = None
    commit = None


class OrderDeparser(astwalk.ASTDeparser):
    """Produce a sort function from a supplied logic.Expression object.
    
    Each positional argument in the Expression's function signature will be
    mapped to 'columns' in the list of lists being sorted.
    """
    
    def __init__(self, expr):
        self.expr = expr
        astwalk.ASTDeparser.__init__(self, expr.ast)
    
    def sort_func(self):
        """Walk self and return a function (for use with list.sort)."""
        root = self.ast.root
        if not isinstance(root, (astwalk.ast.Tuple, astwalk.ast.List)):
            raise ValueError("Attribute AST roots must be Tuple or List, "
                             "not %s" % root.__class__.__name__)
        triples = [self.walk(term) for term in root.getChildren()]
        
        def sort_func(x, y):
            for index, attr, descending in triples:
                xval = getattr(x[index], attr)
                if xval is None:
                    diff = -1
                else:
                    yval = getattr(y[index], attr)
                    if yval is None:
                        diff = 1
                    else:
                        diff = cmp(xval, yval)
                if descending:
                    diff = -diff
                if diff != 0:
                    return diff
            return 0
        return sort_func
    
    def walk(self, node):
        """Walk the AST and return a string of code."""
        nodetype = node.__class__.__name__
        method = getattr(self, "visit_" + nodetype)
        args = node.getChildren()
        if self.verbose:
            self.debug(nodetype, args)
        return method(*args)
    
    def visit_Name(self, name):
        if name in self.ast.args:
            # We've hit a reference to a positional arg, which in our case
            # implies a reference to a Unit class.
            return self.ast.args.index(name)
        else:
            # Since lambdas don't support local bindings,
            # any remaining local name must be a keyword arg.
            raise TypeError("Keyword args not allowed in order expressions.")
    
    def visit_Getattr(self, expr, attrname):
        expr = self.walk(expr)
        if isinstance(expr, int):
            # The name in question refers to a UnitProperty (see visit_Name).
            # The 'False' value declares this is NOT a descending sort.
            # self.func__builtin___reversed might modify that to True.
            return [expr, attrname, False]
        else:
            raise TypeError("%r.%r does not reference a positional argument." %
                            (expr, attrname))
    
    def visit_Const(self, value):
        return value
    
    def visit_CallFunc(self, func, *args):
        # e.g. CallFunc(Name('reversed'), [Getattr(Name('v'), 'Date')], None, None)
        dstar_args = args[-1]
        star_args = args[-2]
        
        posargs = []
        kwargs = {}
        for arg in args[:-2]:
            if isinstance(arg, astwalk.ast.Keyword):
                kwargs[arg.name] = self.walk(arg.value)
            else:
                posargs.append(self.walk(arg))
        
        func = self.walk(func)
        
        # Handle function objects.
        if logic.builtins.get(func.__name__, None) is func:
            dispatch = getattr(self, "builtins_" + func.__name__, None)
            if dispatch:
                return dispatch(*posargs)
        
        funcname = func.__module__ + "_" + func.__name__
        funcname = funcname.replace(".", "_")
        if funcname.startswith("_"):
            funcname = "func" + funcname
        dispatch = getattr(self, funcname, None)
        if dispatch:
            return dispatch(*posargs)
        
        raise CannotRepresent(func)
    
    # --------------------------- Dispatchees --------------------------- #
    
    def func__builtin___reversed(self, x):
        # Assume reversed is always used for DESC ordering.
        # See visit_Getattr for the [expr, attrname, desc] list.
        x[2] = True
        return x
    # For version of Python which did not possess the 'reversed' builtin.
    builtins_reversed = func__builtin___reversed



class ProxyStorage(StorageManager):
    """A Storage Manager which passes calls to another Storage Manager.
    
    database_scope: if True (the default), create_database and drop_database
        calls are passed on to self.nextstore. Set this to False when using
        a Proxy in a cyclic storage graph, where another StorageManager will
        create the proxied database. For example, if you vertically parition
        a set of classes so that some classes are cached and some are not:
        
                            VerticalPartitioner
                                    |   \
                                    |   ObjectCache--RAMStorage
                                    |   /
                                   Master
        
        ...then set this value to False so that the "Master" StorageManager
        receives only one create_database call.
        
        Future versions of Dejavu may grow "table_scope" or other attributes
        which work similarly against create/drop_storage/property.
    """
    
    def __init__(self, allOptions={}):
        StorageManager.__init__(self, allOptions)
        self.nextstore = allOptions.get('Next Store')
        self.database_scope = allOptions.get('database_scope', True)
    
    def unit(self, cls, **kwargs):
        """A single Unit which matches the given kwargs, else None.
        
        The first Unit matching the kwargs is returned; if no Units match,
        None is returned.
        """
        return self.nextstore.unit(cls, **kwargs)
    
    def xrecall(self, classes, expr=None, order=None, limit=None, offset=None):
        """Return an iterable of Units."""
        if limit == 0:
            return
        if offset and not order:
            raise TypeError("Order argument expected when offset is provided.")
        
        if isinstance(classes, dejavu.UnitJoin):
            for unitrow in self._xmultirecall(classes, expr, order=order,
                                              limit=limit, offset=offset):
                yield unitrow
            return
        
        cls = classes
        if self.logflags & logflags.RECALL:
            self.log(logflags.RECALL.message(cls, expr))
        for unit in self.nextstore.xrecall(cls, expr, order=order,
                                           limit=limit, offset=offset):
            yield unit
    
    def save(self, unit, forceSave=False):
        """Store the unit."""
        if self.logflags & logflags.SAVE:
            self.log(logflags.SAVE.message(unit, forceSave))
        self.nextstore.save(unit, forceSave)
    
    def destroy(self, unit):
        """Delete the unit."""
        if self.logflags & logflags.DESTROY:
            self.log(logflags.DESTROY.message(unit))
        self.nextstore.destroy(unit)
    
    def reserve(self, unit):
        """Reserve storage space for the Unit."""
        self.nextstore.reserve(unit)
        
        # Usually we log ASAP, but here we log after
        # the unit has had a chance to get an auto ID.
        if self.logflags & logflags.RESERVE:
            self.log(logflags.RESERVE.message(unit))
    
    def xview(self, query, order=None, limit=None, offset=None, distinct=False):
        """Yield property tuples for the given query."""
        if limit == 0:
            return
        if offset and not order:
            raise TypeError("Order argument expected when offset is provided.")
        
        if not isinstance(query, dejavu.Query):
            query = dejavu.Query(*query)
        
        if self.logflags & logflags.VIEW:
            self.log(logflags.VIEW.message(query, distinct))
        return self.nextstore.xview(query, order=order, limit=limit,
                                    offset=offset, distinct=distinct)
    
    def _xmultirecall(self, classes, expr=None, order=None, limit=None, offset=None):
        """Full inner join units from each class."""
        if self.logflags & logflags.RECALL:
            self.log(logflags.RECALL.message(classes, expr))
        return self.nextstore._xmultirecall(classes, expr, order, limit, offset)
    
    #                               Schemas                               #
    
    def map(self, classes, conflicts='error'):
        """Map classes to internal storage.
        
        conflict: see errors.conflict.
        """
        self.nextstore.map(classes, conflicts=conflicts)
    
    def create_database(self, conflicts='error'):
        """Create internal structures for the entire database.
        
        conflicts: see errors.conflict.
        
        This method will NOT create storage for each class, nor will
        it create any dependent properties or indexes.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("create database"))
        if self.database_scope:
            self.nextstore.create_database(conflicts=conflicts)
    
    def has_database(self):
        """If storage exists for this database, return True."""
        return self.nextstore.has_database()
    
    def drop_database(self, conflicts='error'):
        """Destroy internal structures for the entire database.
        
        conflicts: see errors.conflict.
        
        This method will also drop storage for each class, including
        all properties and indexes.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop database"))
        if self.database_scope:
            self.nextstore.drop_database(conflicts=conflicts)
    
    def create_storage(self, cls, conflicts='error'):
        """Create internal structures for the given class.
        
        conflicts: see errors.conflict.
        
        This method will also create all dependent properties and indexes.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("create storage %r" % cls))
        self.nextstore.create_storage(cls)
    
    def has_storage(self, cls):
        """If storage structures exist for the given class, return True."""
        return self.nextstore.has_storage(cls)
    
    def drop_storage(self, cls, conflicts='error'):
        """Destroy internal structures for the given class.
        
        conflicts: see errors.conflict.
        
        This method will also drop all dependent properties and indexes.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop storage %r" % cls))
        self.nextstore.drop_storage(cls, conflicts=conflicts)
    
    def add_property(self, cls, name, conflicts='error'):
        """Add internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("add property %r %r" % (cls, name)))
        self.nextstore.add_property(cls, name, conflicts=conflicts)
    
    def has_property(self, cls, name):
        """If storage structures exist for the given property, return True."""
        return self.nextstore.has_property(cls, name)
    
    def drop_property(self, cls, name, conflicts='error'):
        """Destroy internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop property %r %r" % (cls, name)))
        self.nextstore.drop_property(cls, name, conflicts=conflicts)
    
    def rename_property(self, cls, oldname, newname, conflicts='error'):
        """Rename internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("rename property %r from %r to %r" %
                                 (cls, oldname, newname)))
        self.nextstore.rename_property(cls, oldname, newname, conflicts=conflicts)
    
    def shutdown(self, conflicts='error'):
        """Shut down all connections to internal storage.
        
        conflicts: see errors.conflict.
        """
        self.nextstore.shutdown(conflicts=conflicts)
    
    def add_index(self, cls, name, conflicts='error'):
        """Add an index to the given property.
        
        conflicts: see errors.conflict.
        """
        self.nextstore.add_index(cls, name, conflicts=conflicts)
    
    def has_index(self, cls, name):
        """If an index exists for the given property, return True."""
        return self.nextstore.has_index(cls, name)
    
    def drop_index(self, cls, name, conflicts='error'):
        """Destroy any index on the given property.
        
        conflicts: see errors.conflict.
        """
        self.nextstore.drop_index(cls, name, conflicts=conflicts)
    
    def start(self, isolation=None):
        if self.nextstore.start:
            self.nextstore.start(isolation)
    
    def rollback(self):
        if self.nextstore.rollback:
            self.nextstore.rollback()
    
    def commit(self):
        if self.nextstore.commit:
            self.nextstore.commit()


class Version(object):
    
    def __init__(self, atoms):
        if isinstance(atoms, (int, float)):
            atoms = str(atoms)
        if isinstance(atoms, basestring):
            import re
            self.atoms = re.split(r'\W', atoms)
        else:
            self.atoms = [str(x) for x in atoms]
    
    def __str__(self):
        return ".".join([str(x) for x in self.atoms])
    
    def __cmp__(self, other):
        cls = self.__class__
        if not isinstance(other, cls):
            # Try to coerce other to a Version instance.
            other = cls(other)
        
        index = 0
        while index < len(self.atoms) and index < len(other.atoms):
            mine, theirs = self.atoms[index], other.atoms[index]
            if mine.isdigit() and theirs.isdigit():
                mine, theirs = int(mine), int(theirs)
            if mine < theirs:
                return -1
            if mine > theirs:
                return 1
            index += 1
        if index < len(other.atoms):
            return -1
        if index < len(self.atoms):
            return 1
        return 0


managers = {
    "aged": "dejavu.storage.caching.AgedCache",
    "cache": "dejavu.storage.caching.ObjectCache",
    "caching": "dejavu.storage.caching.ObjectCache",
    "burned": "dejavu.storage.caching.BurnedCache",
    "proxy": ProxyStorage,
    
    "access": "dejavu.storage.storeado.StorageManagerADO_MSAccess",
    "msaccess": "dejavu.storage.storeado.StorageManagerADO_MSAccess",
    
    "firebird": "dejavu.storage.storefirebird.StorageManagerFirebird",
    "mysql": "dejavu.storage.storemysql.StorageManagerMySQL",
    
    "postgres": "dejavu.storage.storepypgsql.StorageManagerPgSQL",
    "postgresql": "dejavu.storage.storepypgsql.StorageManagerPgSQL",
    "pypgsql": "dejavu.storage.storepypgsql.StorageManagerPgSQL",
    
    "psycopg": "dejavu.storage.storepsycopg.StorageManagerPsycoPg",
    "psycopg2": "dejavu.storage.storepsycopg.StorageManagerPsycoPg",
    
    "ram": "dejavu.storage.storeram.RAMStorage",
    "shelve": "dejavu.storage.storeshelve.StorageManagerShelve",
    "sqlite": "dejavu.storage.storesqlite.StorageManagerSQLite",
    
    "sqlserver": "dejavu.storage.storeado.StorageManagerADO_SQLServer",
    "mssql": "dejavu.storage.storeado.StorageManagerADO_SQLServer",
    
    "folders": "dejavu.storage.storefs.StorageManagerFolders",
    
    "json": "dejavu.storage.storejson.StorageManagerJSON",
    
    "memcache": "dejavu.storage.storememcached.MemcachedStorageManager",
    "memcached": "dejavu.storage.storememcached.MemcachedStorageManager",
    }


def resolve(store, options=None):
    """Return a StorageManager for the given name, classname, class, or SM."""
    if isinstance(store, basestring):
        if store in managers:
            store = managers[store]
    
    if isinstance(store, basestring):
        store = xray.classes(store)(options or {})
    else:
        import types
        if isinstance(store, (type, types.ClassType)):
            store = store(options or {})
    
    return store

