try:
    set
except NameError:
    from sets import Set as set

import dejavu
from dejavu import errors
from geniusql import codewalk, logic


_simple_attr_compare = ''.join(map(chr, (
    codewalk.opmap['LOAD_FAST'], 0, 0,
    codewalk.opmap['LOAD_ATTR'], 1, 0,
    codewalk.opmap['LOAD_CONST'], 1, 0,
    codewalk.opmap['COMPARE_OP'], 2, 0,
    codewalk.opmap['RETURN_VALUE']
    )))


class Sandbox(object):
    """Data sandbox for Dejavu.
    
    Each consumer (that is, each UI process or thread) maintains a Sandbox
    for managing Units. Sandboxes populate themselves with Units on a lazy
    basis, allowing UI code to request data as it's needed. However, once
    obtained, such Units are persisted (usually for the lifetime of the
    thread); this important detail means that multiple requests for the
    same Units result in multiple references to the same objects, rather
    than multiple objects. Sandboxes are basically what Fowler calls
    Identity Maps.
    
    The *REALLY* important thing to understand if you're customizing this
    is that Sandboxes won't survive sharing across threads--DON'T TRY IT.
    If you need to share unit data across requests, use or make an SM which
    persists the data, and chain it with another, more normal SM.
    
    _cache() and _caches are private for a reason--don't access
    them from interface code--tell the Sandbox to do it for you.
    
    Starting with Python 2.5, each Sandbox instance is its own context
    manager, so you can have boxes automatically flush themselves
    when you're done, and automatically rollback on error. Example:
    
    # __future__ only needed for Python 2.5, not 2.6+
    from __future__ import with_statement
    
    with storage.new_sandbox() as box:
        WAP = box.unit(Zoo, Name='Wild Animal Park')
        WAP.Opens = now
    """
    
    def __init__(self, store):
        self.store = store
        self._caches = {}
    
    def __getattr__(self, key):
        # Support "magic recaller" methods on self.
        for cls in self.store.classes:
            name = cls.__name__
            if name == key:
                if cls.identifiers:
                    uniq = cls.identifiers
                else:
                    uniq = cls._properties.keys()
                def recaller(*args, **kwargs):
                    # Allow identifiers to be supplied as args or kwargs
                    # (since the common case will be a single identifier).
                    for arg, key in zip(args, uniq):
                        kwargs[str(key)] = arg
                    return self.unit(cls, **kwargs)
                recaller.__doc__ = "A single %s Unit, else None." % name
                return recaller
        raise AttributeError("Sandbox object has no attribute '%s'" % key)
    
    def memorize(self, *units):
        """Persist the given unit(s) in storage."""
        for unit in units:
            cls = unit.__class__
            unit.sandbox = self
            
            # Ask the store to accept the unit, assigning it primary key values
            # if necessary. The store should also call unit.cleanse() if it
            # saves the whole unit state on this call.
            self.store.reserve(unit)
            
            # Insert the unit into the cache.
            if cls.identifiers:
                uid = unit.identity()
            else:
                # Use id(unit) instead of unit.ID
                uid = id(unit)
            self._cache(cls)[uid] = unit
            
            # Do this at the end of the func, since most on_memorize
            # will want to have an identity when called.
            if hasattr(unit, "on_memorize"):
                unit.on_memorize()
    
    def forget(self, *units):
        """Destroy the given units, both in the cache and storage."""
        for unit in units:
            cls = unit.__class__
            
            if cls.identifiers:
                uid = unit.identity()
            else:
                uid = id(unit)
            del self._cache(cls)[uid]
            
            self.store.destroy(unit)
            
            # This must be done after the destroy() call, so that a
            # related unit can poll all instances of this class.
            if hasattr(unit, "on_forget"):
                unit.on_forget()
            
            unit.sandbox = None
    
    def xrecall(self, classes, expr=None, order=None, limit=None, offset=None):
        """Iterator over units of the given class(es) which match expr.
        
        If the 'classes' arg is a UnitJoin, each yielded value will
        be a list of Units, in the same order as the classes arg.
        This facilitates unpacking in iterative consumer code like:
        
        for invoice, price in sandbox.xrecall(Invoice & Price, f):
            deal_with(invoice)
            deal_with(price)
        
        Recalling multiple classes is currently not well-isolated.
        If an expr argument is supplied, then the store may not return rows
        which our cache would, and those won't be included in the resultset.
        If you're using xrecall with joins, you should be safe if:
        
            * You pass no expr, or
            * You're using this sandbox as read-only, or
            * You call flush_all() after mutating Units but before recalling
                multiple classes.
        """
        if isinstance(classes, dejavu.UnitJoin):
            for unitrow in self._xmultirecall(classes, expr, order=order,
                                              limit=limit, offset=offset):
                yield unitrow
            return
        
        cls = classes
        if not isinstance(expr, logic.Expression):
            expr = logic.Expression(expr)
        
        cache = self._cache(cls)
        
        # Special-case the scenario where one Unit is expected
        # and called by ID. We should be able to save a database hit.
        if expr:
            fc = expr.func.func_code
            if (fc.co_code == _simple_attr_compare and
                    cls.identifiers == (fc.co_names[-1], )):
                ID = fc.co_consts[-1]
                unit = cache.get((ID,))
                if unit is not None:
                    # Do NOT call on_recall here. That should be called
                    # only at the Sandbox-SM boundary.
                    yield unit
                    return
        
        if limit:
            offset = offset or 0
            limit = offset + limit
        elif limit == 0:
            return
        
        keys = []
        if order:
            # If an order is supplied, there's no point in running the
            # query against our cache (because we'd have to interleave
            # the results with those from storage anyway). We'll still
            # prefer sandboxed units over those retrieved from storage
            # (see below), but we'll pass for now.
            pass
        elif offset:
            raise TypeError("Order argument expected when offset is provided.")
        elif cls.identifiers:
            # Query the cache. We have to use a static copy of the
            # keys, to ensure that our cache doesn't change size
            # during iteration (due to overlapping xrecalls).
            keys = cache.keys()
            for id in keys:
                unit = cache.get(id)
                if unit and ((expr is None) or expr.evaluate(unit)):
                    # Do NOT call on_recall here. That should be called
                    # only at the Sandbox-SM boundary.
                    yield unit
                    if limit:
                        limit -= 1
                        if limit == 0:
                            return
        
        # Query storage.
        if not cls.identifiers:
            # Classes with no identifiers cannot be compared to our cache
            for unit in self.store.xrecall(cls, expr, order=order,
                                           limit=limit, offset=offset):
                unit.sandbox = self
                if hasattr(unit, 'on_recall'):
                    try:
                        unit.on_recall()
                    except errors.UnrecallableError:
                        continue
                yield unit
        else:
            for unit in self.store.xrecall(cls, expr, order=order,
                                           limit=limit, offset=offset):
                id = unit.identity()
                # Don't offer up a unit that was already checked in our cache
                # (whether it matched the expr() or not--we assume the cache
                # has the freshest data).
                if id in keys:
                    # We've already yielded this unit.
                    pass
                else:
                    # Very important that we check for an existing unit in
                    # the sandbox cache, as its state may have changed in
                    # memory but not in storage (even between our cache
                    # yields and this yield).
                    # Make sure the cache lookup and get happens atomically.
                    existing = cache.get(id)
                    if existing:
                        yield existing
                    else:
                        unit.sandbox = self
                        cache[id] = unit
                        if hasattr(unit, 'on_recall'):
                            try:
                                unit.on_recall()
                            except errors.UnrecallableError:
                                continue
                        yield unit
    
    def recall(self, classes, expr=None, order=None, limit=None, offset=None):
        """List of units of the given class(es) which match expr.
        
        If the 'classes' arg is a UnitJoin, each yielded value will
        be a list of Units, in the same order as the classes arg.
        This facilitates unpacking in iterative consumer code like:
        
        for invoice, price in sandbox.recall(Invoice & Price, f):
            deal_with(invoice)
            deal_with(price)
        
        Recalling multiple classes is currently not well-isolated.
        If an expr argument is supplied, then the store may not return rows
        which our cache would, and those won't be included in the resultset.
        If you're using recall with joins, you should be safe if:
        
            * You pass no expr, or
            * You're using this sandbox as read-only, or
            * You call flush_all() after mutating Units but before recalling
                multiple classes.
        """
        return [x for x in self.xrecall(classes, expr, order=order,
                                        limit=limit, offset=offset)]
    
    def unit(self, cls, **kwargs):
        """A single Unit which matches the given kwargs, else None.
        
        The first Unit matching the kwargs is returned; if no Units match,
        None is returned.
        """
        cache = self._cache(cls)
        
        # Special-case the scenario where one Unit is expected
        # and called by ID. We should be able to save a database hit.
        if set(kwargs.keys()) == set(cls.identifiers):
            ident = tuple([kwargs[k] for k in cls.identifiers])
            if ident:
                u = cache.get(ident)
                if u is None:
                    u = self.store.unit(cls, **kwargs)
                    if u is not None:
                        u.sandbox = self
                        cache[ident] = u
                        if hasattr(u, 'on_recall'):
                            try:
                                u.on_recall()
                            except errors.UnrecallableError:
                                return None
                return u
        
        # Query the cache. We have to use a static copy of the
        # keys, to ensure that our cache doesn't change size
        # during iteration (due to overlapping xrecalls).
        keys = cache.keys()
        for id in keys:
            u = cache.get(id)
            if u:
                for k, v in kwargs.iteritems():
                    if getattr(u, k) != v:
                        break
                else:
                    # Do NOT call on_recall here. That should be called
                    # only at the Sandbox-SM boundary.
                    return u
        
        # Query Storage.
        u = self.store.unit(cls, **kwargs)
        if u is not None:
            u.sandbox = self
            
            # Classes with no identifiers cannot be compared to our cache
            if cls.identifiers:
                # Very important that we check for existing unit, as its
                # state may have changed in memory but not in storage
                # (even between our cache lookups and this lookup).
                # Make sure the cache lookup and get happens atomically.
                id = u.identity()
                existing = cache.get(id)
                if existing:
                    return existing
                cache[id] = u
            
            if hasattr(u, 'on_recall'):
                try:
                    u.on_recall()
                except errors.UnrecallableError:
                    return None
        return u
    
    def _xmultirecall(self, classes, expr=None,
                      order=None, limit=None, offset=None):
        """Recall units of each cls if they together match the expr.
        
        The 'classes' arg must be a UnitJoin, and each yielded value
        will be a list of Units, in the same order as the classes arg.
        This facilitates unpacking in iterative consumer code like:
        
        for invoice, price in sandbox.recall(Invoice & Price, f):
            deal_with(invoice)
            deal_with(price)
        
        Recalling multiple classes is currently not well-isolated.
        If an expr argument is supplied, then the store may not return rows
        which our cache would, and those won't be included in the resultset.
        If you're using recall with joins, you should be safe if:
        
            * You pass no expr, or
            * You're using this sandbox as read-only, or
            * You call flush_all() after mutating Units but before recalling
                multiple classes.
        """
        if not isinstance(expr, logic.Expression):
            expr = logic.Expression(expr)
        
        # This is broken. If a filter expr is supplied, then the store may
        # not return rows which our cache would, and those won't be included
        # in the resultset. If you're using xmulti with no expr's, or
        # in read-only scripts, it should be OK for now. But if you mutate
        # Units and then call _xmultirecall, expect inconsistent results.
        for unitset in self.store._xmultirecall(classes, expr, order=order,
                                                limit=limit, offset=offset):
            confirmed = True
            for index in xrange(len(unitset)):
                unit = unitset[index]
                id = unit.identity()
                if not unit.sequencer.valid_id(id):
                    # This is a 'dummy unit' from an outer join.
                    continue
                cache = self._cache(unit.__class__)
                if id in cache:
                    # Keep the unit which is in our cache!
                    unitset[index] = cache[id]
                else:
                    cache[id] = unit
                    unit.sandbox = self
                    if hasattr(unit, 'on_recall'):
                        try:
                            unit.on_recall()
                        except errors.UnrecallableError:
                            confirmed = False
                            break
            if confirmed:
                yield unitset
    
    def xview(self, query, distinct=False):
        """Yield tuples of attrs for the given Query.
        
        Each yielded value will be a list of values, in the same order as
        the Query attributes. This facilitates unpacking in iterative
        consumer code like:
        
        for id, name in sandbox.xview(Query(Invoice, ['ID', 'Name'], f)):
            print id, ": ", name
        
        This is generally much faster than recall, and should be preferred
        for performance-sensitive code.
        """
        if not isinstance(query, dejavu.Query):
            query = dejavu.Query(*query)
        
        expr = query.restriction
        attrs = query.attributes
        if isinstance(query.relation, dejavu.UnitJoin):
            classes = query.relation
        else:
            classes = [query.relation]
        
        # Add the identity attribute(s) if not present. This is necessary
        # to avoid duplicating objects which are already in our cache.
        if isinstance(query.attributes, logic.Expression):
            # TODO: add support for this
            choke
        elif isinstance(query.relation, dejavu.UnitJoin):
            # TODO: add support for this
            choke
        else:
            cls = query.relation
            fields = list(query.attributes)
            indices = []
            added_fields = 0
            for key in cls.identifiers:
                if key not in fields:
                    added_fields += 1
                    fields.append(key)
                indices.append(fields.index(key))
        
        seen = {}
        
        for cls in classes:
            cache = self._cache(cls)
            for unit in cache.itervalues():
                if expr is None or expr(unit):
                    datarow = tuple([getattr(unit, attr) for attr in attrs])
                    if distinct:
                        if datarow not in seen:
                            yield datarow
                            seen[datarow] = None
                    else:
                        yield datarow
        
        for datarow in self.store.xview((query.relation, fields, expr), distinct=distinct):
            id = tuple([datarow[x] for x in indices])
            if id not in cache:
                if added_fields:
                    # Remove the added identifier columns from the row.
                    datarow = datarow[:-added_fields]
                
                if distinct:
                    if datarow not in seen:
                        yield datarow
                        seen[datarow] = None
                else:
                    yield datarow
    
    def view(self, query, distinct=False):
        """Return tuples of attrs for the given Query."""
        return [x for x in self.xview(query, distinct=distinct)]
    
    def sum(self, cls, attr, expr=None):
        """Sum of all non-None values for the given cls.attr."""
        expr = logic.Expression(lambda x: getattr(x, attr) != None) + expr
        return sum([row[0] for row in
                    self.view(dejavu.Query(cls, (attr,), expr))])
    
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
        existing = [x[0] for x in self.view(query, distinct=True)
                    if x is not None]
        if not existing:
            return []
        
        attr_type = getattr(cls, attr).type
        if issubclass(attr_type, (int, long)):
            return range(min(existing), max(existing) + 1)
        else:
            try:
                import datetime
            except ImportError:
                pass
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
    
    #                           Cache Management                           #
    
    def _cache(self, cls):
        """Return the cache for the specified class.
        
        This base class creates a new cache for each cls per request.
        """
        if cls not in self._caches:
            self._caches[cls] = {}
        return self._caches[cls]
    
    def purge(self, cls):
        """Drop all cached Units of class 'cls'. Do not save."""
        del self._caches[cls]
    
    def repress(self, *units):
        """Remove units from cache (but don't destroy)."""
        for unit in units:
            cls = unit.__class__
            
            if cls.identifiers:
                uid = unit.identity()
            else:
                uid = id(unit)
            
            if hasattr(unit, "on_repress"):
                unit.on_repress()
            
            # Save after on_repress in case on_repress modified the unit.
            self.store.save(unit)
            
            del self._cache(cls)[uid]
            
            unit.sandbox = None
    
    def flush_all(self):
        """Repress all units and commit any open transaction."""
        
        for cls in self._caches.keys():
            # Call all on_repress methods first! There are truly horrible
            # interdependency chains in most on_repress methods, and
            # it's best to resolve them all at once BEFORE flushing
            # any units from the cache.
            # Note we use values instead of itervalues, since the
            # cache may change size during iteration.
            for unit in self._cache(cls).values():
                if hasattr(unit, "on_repress"):
                    unit.on_repress()
        
        for cls in self._caches.keys():
            cache = self._cache(cls)
            while cache:
                unitid, unit = cache.popitem()
                self.store.save(unit)
        
        self.commit()
    
    #                        Transaction Management                        #
    
    def start(self, isolation=None):
        """Start a transaction."""
        if self.store.start:
            self.store.start(isolation)
    
    def commit(self):
        """Commit the current transaction.
        
        If errors occur during this process, they are not trapped here.
        You must either call rollback yourself (or fix the problem and
        try to commit again).
        """
        if self.store.commit:
            self.store.commit()
    
    def rollback(self):
        """Roll back the current transaction (all changes) and purge our cache."""
        for cls in self._caches.keys():
            # Dump all objects in this cache
            self.purge(cls)
        
        if self.store.rollback:
            self.store.rollback()
    
    #                          Context Management                          #
    
    def __enter__(self):
        return self
    
    def __exit__ (self, type, value, tb):
        if tb is None:
            self.flush_all()
        else:
            self.rollback()

