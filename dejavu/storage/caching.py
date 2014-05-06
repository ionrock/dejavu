"""Caching Storage Managers for Dejavu."""

import datetime
import dejavu
from dejavu import logic, logflags, recur
from dejavu.storage import ProxyStorage, resolve


class ObjectCache(ProxyStorage):
    """A Proxy Storage Manager which recalls and keeps Units in memory.
    
    The recall, reserve, and save methods place units in the cache;
    the destroy method invalidates those entries. Units which exist in the
    cache perfectly reflect the units in the next store; any modifications
    are immediately written to the next store. Unit classes which have
    no identifiers are not cached.
    
    This is primarily designed to be used in OLTP environments, and then
    only for a small subset of classes for which a sizable number of
    objects are usually recalled by ID but rarely modified.
    
    Options:
        
        cache: if given, this should be a StorageManager instance for the
            store which will actually retain cached data (the ObjectCache
            is just a dispatcher). If not given, the default cache will be
            an instance of storeram.RAMStorage.
        
        fullquery: if True, run recall queries against the cache before
            checking storage. When using key-value caches (like memcached),
            this can be slow and should be turned off. If False (the default),
            recall will still place recalled units into the cache.
        
        fulljoin: if True, perform recalls involving multiple classes using
            the cache. This can be quite slow when the involved classes are
            large. If False (the default), multirecall will skip reading
            the cache and query self.nextstore directly.
    """
    
    def __init__(self, allOptions={}):
        ProxyStorage.__init__(self, allOptions)
        
        self.fullquery = allOptions.get("fullquery", False)
        self.fulljoin = allOptions.get("fulljoin", False)
        
        self.cache = allOptions.get("cache")
        if self.cache is None:
            self.cache = resolve("ram")
    
    def unit(self, cls, **kwargs):
        """A single Unit which matches the given kwargs, else None.
        
        The first Unit matching the kwargs is returned; if no Units match,
        None is returned.
        """
        if cls in self.cache.classes:
            u = self.cache.unit(cls, **kwargs)
            if u is not None:
                return u
        
        u = self.nextstore.unit(cls, **kwargs)
        if u is not None:
            try:
                self.cache.save(u, forceSave=True)
            except KeyError:
                # The cache refused to save the unit (possibly full).
                pass
        
        return u
    
    def xrecall(self, classes, expr=None, order=None, limit=None, offset=None):
        """Return a Unit iterator."""
        if isinstance(classes, dejavu.UnitJoin):
            for unitrow in self._xmultirecall(classes, expr, order=order,
                                              limit=limit, offset=offset):
                yield unitrow
                return
        
        cls = classes
        # Units which have no identifiers are not cached
        if not cls.identifiers:
            for unit in ProxyStorage.xrecall(self, cls, expr, order=order,
                                             limit=limit, offset=offset):
                yield unit
                return
        
        if not isinstance(expr, logic.Expression):
            expr = logic.Expression(expr)
        if self.logflags & logflags.RECALL:
            self.log(logflags.RECALL.message(cls, expr))
        
        if limit == 0:
            return
        
        seen = {}
        if order:
            # If an order is supplied, there's no point in running the
            # query against our cache (because we'd have to interleave
            # the results with those from storage anyway).
            pass
        elif offset:
            raise TypeError("Order argument expected when offset is provided.")
        elif self.fullquery and cls.identifiers and cls in self.cache.classes:
            # Query the cache.
            for unit in self.cache.xrecall(cls, expr, limit=limit, offset=offset):
                seen[unit.identity()] = None
                yield unit
            limit = limit - len(keys)
        
        # Query storage.
        if cls in self.cache.classes:
            for unit in self.nextstore.xrecall(cls, expr, order=order,
                                               limit=limit, offset=offset):
                id = unit.identity()
                # Don't offer up a unit we already yielded from the cache.
                if id not in seen:
                    try:
                        self.cache.save(unit, forceSave=True)
                    except KeyError:
                        # The cache refused to save the unit (possibly full).
                        pass
                    seen[id] = None
                    yield unit
        else:
            for unit in self.nextstore.xrecall(cls, expr, order=order,
                                               limit=limit, offset=offset):
                yield unit
    
    def _xmultirecall(self, classes, expr=None, order=None, limit=None, offset=None):
        """Yield lists of units of the given classes which match expr."""
        if self.fulljoin:
            # Use the superclass' _combine method. This can be VERY slow,
            # since ALL objects of each class will be read into memory,
            # combined, and tested against the expression.
            for unitrow in ProxyStorage._xmultirecall(
                self, classes, expr, order=order, limit=limit, offset=offset):
                yield unitrow
        else:
            # Skip reading from the cache (since it probably has poor
            # performance for this kind of operation). But we'll still
            # *write* to the cache.
            seen = [{} for cls in classes]
            for unitrow in self.nextstore._xmultirecall(
                classes, expr, order=order, limit=limit, offset=offset):
                for i, unit in enumerate(unitrow):
                    ident = unit.identity()
                    if not unit.sequencer.valid_id(ident):
                        # This is a 'dummy unit' from an outer join.
                        continue
                    if ident not in seen[i]:
                        try:
                            self.cache.save(unit, forceSave=True)
                        except KeyError:
                            # The cache refused to save the unit (possibly full).
                            pass
                        seen[i][ident] = None
                yield unitrow
    
    def save(self, unit, forceSave=False):
        """Store the unit."""
        if not unit.identifiers:
            return ProxyStorage.save(self, unit, forceSave)
        
        if self.logflags & logflags.SAVE:
            self.log(logflags.SAVE.message(unit, forceSave))
        
        # nextstore might call unit.cleanse()
        update_cache = (unit.__class__ in self.cache.classes
                        and (forceSave or unit.dirty()))
        self.nextstore.save(unit, forceSave)
        if update_cache:
            try:
                self.cache.save(unit, forceSave=update_cache)
            except KeyError:
                # The cache refused to save the unit (possibly full).
                pass
    
    def destroy(self, unit):
        """Delete the unit."""
        if not unit.identifiers:
            return ProxyStorage.destroy(self, unit)
        
        if self.logflags & logflags.DESTROY:
            self.log(logflags.DESTROY.message(unit))
        
        self.nextstore.destroy(unit)
        self.invalidate(unit)
    
    def reserve(self, unit):
        """Reserve storage space for the Unit."""
        if not unit.identifiers:
            return ProxyStorage.reserve(self, unit)
        
        # Allow the proxied store to set any auto-ID's
        self.nextstore.reserve(unit)
        
        if unit.__class__ in self.cache.classes and not unit.dirty():
            try:
                self.cache.reserve(unit)
            except KeyError:
                # The cache refused to save the unit (possibly full).
                pass
        
        if self.logflags & logflags.RESERVE:
            self.log(logflags.RESERVE.message(unit))
    
    def invalidate(self, unit):
        if unit.identifiers and unit.__class__ in self.cache.classes:
            self.cache.destroy(unit)
    
    def shutdown(self, conflicts='error'):
        """Shut down all connections to internal storage.
        
        conflicts: see errors.conflict.
        """
        self.cache.shutdown(conflicts=conflicts)
        self.nextstore.shutdown(conflicts=conflicts)
    
    def create_database(self, conflicts='error'):
        """Create internal structures for the entire database.
        
        conflicts: see errors.conflict.
        
        This method will NOT create storage for each class, nor will
        it create any dependent properties or indexes.
        """
        ProxyStorage.create_database(self, conflicts=conflicts)
        self.cache.create_database(conflicts=conflicts)
    
    def drop_database(self, conflicts='error'):
        """Destroy internal structures for the entire database.
        
        conflicts: see errors.conflict.
        
        This method will also drop storage for each class, including
        all properties and indexes.
        """
        ProxyStorage.drop_database(self, conflicts=conflicts)
        self.cache.drop_database(conflicts=conflicts)
    
    def create_storage(self, cls, conflicts='error'):
        """Create internal structures for the given class.
        
        conflicts: see errors.conflict.
        
        This method will also create all dependent properties and indexes.
        """
        ProxyStorage.create_storage(self, cls, conflicts=conflicts)
        if cls in self.cache.classes:
            self.cache.create_storage(cls, conflicts=conflicts)
    
    def drop_storage(self, cls, conflicts='error'):
        """Destroy internal structures for the given class.
        
        conflicts: see errors.conflict.
        
        This method will also drop all dependent properties and indexes.
        """
        ProxyStorage.drop_storage(self, cls, conflicts=conflicts)
        if cls in self.cache.classes:
            self.cache.drop_storage(cls, conflicts=conflicts)
    
    def add_property(self, cls, name, conflicts='error'):
        """Add internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("add property %r %r" % (cls, name)))
        self.nextstore.add_property(cls, name, conflicts=conflicts)
        if cls in self.cache.classes:
            self.cache.add_property(cls, name, conflicts=conflicts)
    
    def drop_property(self, cls, name, conflicts='error'):
        """Destroy internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop property %r %r" % (cls, name)))
        self.nextstore.drop_property(cls, name, conflicts=conflicts)
        if cls in self.cache.classes:
            self.cache.drop_property(cls, name, conflicts=conflicts)
    
    def rename_property(self, cls, oldname, newname, conflicts='error'):
        """Rename internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("rename property %r from %r to %r"
                                 % (cls, oldname, newname)))
        self.nextstore.rename_property(cls, oldname, newname, conflicts=conflicts)
        if cls in self.cache.classes:
            self.cache.rename_property(cls, oldname, newname, conflicts=conflicts)
    
    def map(self, classes, conflicts='error'):
        """Map classes to internal storage.
        
        conflicts: see errors.conflict.
        """
        ProxyStorage.map(self, classes, conflicts=conflicts)
        
        classes = set(classes).intersection(self.cache.classes)
        if classes:
            self.cache.map(classes, conflicts=conflicts)
    
    def start(self, isolation=None):
        ProxyStorage.start(self, isolation)
        if self.cache.start:
            self.cache.start(isolation)
    
    def rollback(self):
        ProxyStorage.rollback(self)
        if self.cache.rollback:
            self.cache.rollback()
    
    def commit(self):
        ProxyStorage.commit(self)
        if self.cache.commit:
            self.cache.commit()


class AgedCache(ObjectCache):
    """A Proxy Storage Manager which recalls and keeps Units in memory.
    
    The recall, reserve, and save methods place units in the cache;
    the destroy method invalidates those entries. Units which exist in the
    cache perfectly reflect the units in the next store; any modifications
    are immediately written to the next store. Unit classes which have
    no identifiers are not cached. DDL methods empty all involved caches.
    
    This is primarily designed to be used in OLTP environments, and then
    only for a small subset of classes for which a sizable number of
    objects are usually recalled by ID but rarely modified.
    
    The 'lifetime' option should be the number of seconds (a float)
    between "sweeps". Each sweep checks the last recall date of all
    objects in the cache, invalidating any that have been idle longer
    than the given lifetime. Note that the sweeper Worker is not started
    for you; you must either call sm.sleeper.start() at regular intervals,
    or use a recur.Scheduler to cycle it for you.
    """
    
    def __init__(self, allOptions={}):
        ObjectCache.__init__(self, allOptions)
        
        self._recallTimes = {}  # {cls: {id: datetime.datetime}}
        # Create and motivate a worker to sweep out idle Units.
        lifetime = allOptions.get('Lifetime', '')
        if lifetime:
            
            class IdleSweeper(recur.Worker):
                """A worker to sweep out idle Units."""
                def work(me):
                    """Start a cycle of scheduled work."""
                    # Note that 'self' refers to the Proxy, not the Worker.
                    self.sweep_all()
            self.sweeper = IdleSweeper(lifetime)
    
    def map(self, classes, conflicts='error'):
        """Map classes to internal storage.
        
        conflicts: see errors.conflict.
        """
        ProxyStorage.map(self, classes, conflicts=conflicts)
        
        classes = set(classes).intersection(self.cache.classes)
        if classes:
            self.cache.map(classes, conflicts=conflicts)
            for cls in classes:
                self._recallTimes.setdefault(cls, {})
    
    def xrecall(self, classes, expr=None, order=None, limit=None, offset=None):
        """Return a Unit iterator."""
        if isinstance(classes, dejavu.UnitJoin):
            for unitrow in self._xmultirecall(classes, expr, order=order,
                                              limit=limit, offset=offset):
                yield unitrow
            return
        
        cls = classes
        if cls in self.cache.classes:
            start = datetime.datetime.now()
            recallTimes = self._recallTimes[cls]
            for unit in ObjectCache.xrecall(self, cls, expr,
                                            order, limit, offset):
                recallTimes[unit.identity()] = start
                yield unit
        else:
            for unit in ObjectCache.xrecall(self, cls, expr,
                                            order, limit, offset):
                yield unit
    
    def reserve(self, unit):
        """Reserve storage space for the Unit."""
        ObjectCache.reserve(self, unit)
        cls = unit.__class__
        if cls in self.cache.classes:
            recallTimes = self._recallTimes[cls]
            recallTimes[unit.identity()] = datetime.datetime.now()
    
    def invalidate(self, unit):
        if unit.identifiers:
            cls = unit.__class__
            if cls in self.cache.classes:
                ObjectCache.invalidate(self, unit)
                try:
                    del self._recallTimes[cls][unit.identity()]
                except KeyError:
                    pass
    
    def sweep(self, cls, lastSweepTime=None):
        """Sweep idle units out of the cache for the given class."""
        recallTimes = self._recallTimes[cls]
        for unit in self.cache.xrecall(cls):
            id = unit.identity()
            lastRecall = recallTimes.setdefault(id, None)
            if (lastRecall is None or lastSweepTime is None or lastRecall < lastSweepTime):
                self.invalidate(unit)
    
    def sweep_all(self, lastSweepTime=None):
        """Sweep idle units out of the cache for all classes."""
        for cls in self.cache.classes:
            self.sweep(cls, lastSweepTime)


class BurnedCache(ObjectCache):
    """An Object Cache which recalls and caches ALL Units.
    
    The big performance difference for a burned cache is that, once _any_
    Units have been recalled, all Units for that class are placed in the
    cache, so further recalls won't hit the next store unless the cache
    is completely emptied.
    
    Notice we didn't say "performance _benefit_" ;) That would depend to
    a great extent on the proxied store.
    
    This should NOT be used with lossy caches like memcached, since it
    depends on always having a complete cache of a given class.
    """
    
    def xrecall(self, classes, expr=None, order=None, limit=None, offset=None):
        """Return a Unit iterator."""
        if isinstance(classes, dejavu.UnitJoin):
            return self._xmultirecall(classes, expr, order=order,
                                      limit=limit, offset=offset)
        
        cls = classes
        # Units which have no identifiers are not cached
        if not cls.identifiers:
            return ProxyStorage.xrecall(self, cls, expr, order=order,
                                        limit=limit, offset=offset)
        else:
            if self.logflags & logflags.RECALL:
                self.log(logflags.RECALL.message(cls, expr))
            
            # If the cache is empty, refill it completely; otherwise,
            # assume it's completely in sync with the next store.
            # Assumes the cache has the nonstandard 'cachelen' method.
            if cls in self.cache.classes and not self.cache.cachelen(cls):
                try:
                    # The missing 'expr' below is not a bug: we want ALL Units.
                    for unit in self.nextstore.xrecall(cls):
                        self.cache.save(unit, forceSave=True)
                except KeyError:
                    # The cache refused to save the unit (possibly full).
                    for unit in self.cache.cached_units(cls):
                        self.cache.destroy(unit)
            
            return self.cache.xrecall(cls, expr, order, limit, offset)

