try:
    import cPickle as pickle
except ImportError:
    import pickle

import thread

import dejavu
from dejavu import errors, logic, logflags, storage


class RAMStorage(storage.StorageManager):
    """A Storage Manager which keeps all data in RAM."""
    
    def __init__(self, allOptions={}):
        storage.StorageManager.__init__(self, allOptions)
        self._caches = {}       # id: pickled Unit
        self._cache_locks = {}
    
    def _get_lock(self, cls):
        lock = self._cache_locks[cls]
        lock.acquire(True)
        return lock
    
    def unit(self, cls, **kwargs):
        """A single Unit which matches the given kwargs, else None.
        
        The first Unit matching the kwargs is returned; if no Units match,
        None is returned.
        """
        if self.logflags & logflags.RECALL:
            self.log(logflags.RECALL.message(cls, kwargs))
        
        lock = self._get_lock(cls)
        try:
            cache = self._caches[cls]
            
            if set(kwargs.keys()) == set(cls.identifiers):
                # Looking up a Unit by its identifiers.
                # Skip grabbing the cached class index (a HUGE optimization).
                id = tuple([kwargs[k] for k in cls.identifiers])
                pickledUnit = cache.get(id)
                if pickledUnit is None:
                    return None
                else:
                    u = pickle.loads(pickledUnit)
                    u.cleanse()
                    return u
            
            try:
                expr = logic.filter(**kwargs)
                return self._xrecall_inner(cache, cache.keys(), expr
                                           ).next()[0]
            except StopIteration:
                return None
        finally:
            lock.release()
    
    def xrecall(self, classes, expr=None, order=None, limit=None, offset=None):
        """Yield units of the given cls which match the given expr."""
        if isinstance(classes, dejavu.UnitJoin):
            return self._xmultirecall(classes, expr, order=order,
                                      limit=limit, offset=offset)
        
        cls = classes
        if self.logflags & logflags.RECALL:
            self.log(logflags.RECALL.message(cls, expr))
        
        lock = self._get_lock(cls)
        try:
            cache = self._caches[cls]
            ids = cache.keys()
            
            if not isinstance(expr, logic.Expression):
                expr = logic.Expression(expr)
            
            data = self._xrecall_inner(cache, ids, expr)
            return self._paginate(data, order, limit, offset, single=True)
        finally:
            lock.release()
    
    def _xrecall_inner(self, cache, ids, expr=None):
        """Private helper for self.xrecall."""
        for id in ids:
            pickledUnit = cache.get(id)
            if pickledUnit is not None:
                unit = pickle.loads(pickledUnit)
                if expr is None or expr(unit):
                    unit.cleanse()
                    # Must yield a sequence for use in _paginate.
                    yield (unit,)
    
    def save(self, unit, forceSave=False):
        """save(unit, forceSave=False). -> Update storage from unit's data."""
        if self.logflags & logflags.SAVE:
            self.log(logflags.SAVE.message(unit, forceSave))
        
        if forceSave or unit.dirty():
            lock = self._get_lock(unit.__class__)
            try:
                cache = self._caches[unit.__class__]
                if unit.identifiers:
                    # Replace the entire value to get around writeback issues.
                    # See the docs on "shelve" for more info.
                    key = unit.identity()
                else:
                    # This class has no identifiers, so hash the whole dict.
                    key = pickle.dumps(unit._properties)
                
                # Cleanse first because pickle state
                # includes _initial_property_hash.
                unit.cleanse()
                cache[key] = pickle.dumps(unit)
            finally:
                lock.release()
    
    def destroy(self, unit):
        """Delete the unit."""
        if self.logflags & logflags.DESTROY:
            self.log(logflags.DESTROY.message(unit))
        
        cls = unit.__class__
        lock = self._get_lock(cls)
        try:
            cache = self._caches[cls]
            if unit.identifiers:
                id = unit.identity()
            else:
                # This class has no identifiers, so hash the whole dict.
                id = pickle.dumps(unit._properties)
            
            try:
                del cache[id]
            except KeyError:
                pass
        finally:
            lock.release()
    
    def reserve(self, unit):
        """Reserve storage space for the Unit."""
        if unit.identifiers:
            cls = unit.__class__
            lock = self._get_lock(cls)
            try:
                cache = self._caches[cls]
                if not unit.sequencer.valid_id(unit.identity()):
                    unit.sequencer.assign(unit, cache.keys())
                # Pickle the Unit to discard extraneous attributes,
                # and avoid identity issues.
                # Cleanse first because pickle state
                # includes _initial_property_hash.
                unit.cleanse()
                cache[unit.identity()] = pickle.dumps(unit)
            finally:
                lock.release()
        else:
            # This class has no identifiers, so skip reserve and wait for save.
            pass
        
        # Usually we log ASAP, but here we log after
        # the unit has had a chance to get an auto ID.
        if self.logflags & logflags.RESERVE:
            self.log(logflags.RESERVE.message(unit))
    
    def shutdown(self, conflicts='error'):
        """Shut down all connections to internal storage.
        
        conflicts: see errors.conflict.
        """
        self._caches = {}
        self._cache_locks = {}
    
    def create_database(self, conflicts='error'):
        """Create internal structures for the entire database.
        
        conflicts: see errors.conflict.
        """
        pass
    
    def has_database(self):
        """If storage exists for this database, return True."""
        return True
    
    def drop_database(self, conflicts='error'):
        """Destroy internal structures for the entire database.
        
        conflicts: see errors.conflict.
        """
        self.shutdown(conflicts=conflicts)
    
    def create_storage(self, cls, conflicts='error'):
        """Create internal structures for the given class.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("create storage %s" % cls))
        
        if cls in self._caches or cls in self._cache_locks:
            errors.conflict(conflicts, "Class %r already has storage." % cls)
        
        self._caches[cls] = {}
        self._cache_locks[cls] = thread.allocate_lock()
    
    def has_storage(self, cls):
        """If storage structures exist for the given class, return True."""
        return cls in self._caches
    
    def drop_storage(self, cls, conflicts='error'):
        """Destroy internal structures for the given class.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop storage %s" % cls))
        
        try:
            del self._caches[cls]
            del self._cache_locks[cls]
        except KeyError, x:
            errors.conflict(conflicts, str(x))
    
    def add_property(self, cls, name, conflicts='error'):
        """Add internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("add property %s %s" % (cls, name)))
        
        try:
            cache = self._caches[cls]
            lock = self._get_lock(cls)
        except KeyError, x:
            errors.conflict(conflicts, str(x))
        
        try:
            for id, pickledUnit in cache.items():
                unit = pickle.loads(pickledUnit)
                unit._properties[name] = None
                unit.cleanse()
                cache[id] = pickle.dumps(unit)
        finally:
            lock.release()
    
    def has_property(self, cls, name):
        """If storage structures exist for the given property, return True."""
        try:
            cache = self._caches[cls]
            lock = self._get_lock(cls)
        except KeyError, x:
            errors.conflict(conflicts, str(x))
        
        try:
            if not cache:
                # We don't have any items, so there's nothing to
                # declare as 'unprepared'.
                return True
            
            for id, pickledUnit in cache.iteritems():
                unit = pickle.loads(pickledUnit)
                return name in unit._properties
        finally:
            lock.release()
    
    def drop_property(self, cls, name, conflicts='error'):
        """Destroy internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop property %s %s" % (cls, name)))
        
        try:
            cache = self._caches[cls]
            lock = self._get_lock(cls)
        except KeyError, x:
            errors.conflict(conflicts, str(x))
        
        try:
            for id, pickledUnit in cache.items():
                unit = pickle.loads(pickledUnit)
                del unit._properties[name]
                unit.cleanse()
                cache[id] = pickle.dumps(unit)
        finally:
            lock.release()
    
    def rename_property(self, cls, oldname, newname, conflicts='error'):
        """Rename internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("rename property %s from %s to %s"
                                          % (cls, oldname, newname)))
        
        try:
            cache = self._caches[cls]
            lock = self._get_lock(cls)
        except KeyError, x:
            errors.conflict(conflicts, str(x))
        
        try:
            for id, pickledUnit in cache.items():
                unit = pickle.loads(pickledUnit)
                unit._properties[newname] = unit._properties[oldname]
                del unit._properties[oldname]
                unit.cleanse()
                cache[id] = pickle.dumps(unit)
        finally:
            lock.release()
    
    
    #                   Extra methods for use as a cache                   #
    
    def cachelen(self, cls):
        return len(self._caches.get(cls, {}))
    
    def cached_units(self, cls):
        return [pickle.loads(data) for data
                in self._caches.get(cls, {}).itervalues()]
    
    def flush(self, cls):
        """Dump all objects of the given class."""
        lock = self._get_lock(cls)
        try:
            self._caches[cls] = {}
        finally:
            lock.release()

