import md5
import memcache

try:
    set
except NameError:
    from sets import Set as set

import thread
import warnings

import dejavu
from dejavu import errors, logflags, storage
from geniusql import logic


class MemcachedStorageManager(storage.StorageManager):
    """A Storage Manager which keeps all data in memcached.
    
    memcached is a high-performance, distributed memory object caching
    system, generic in nature, but intended for use in speeding up
    dynamic web applications by alleviating database load.
    
    See http://www.danga.com/memcached/
    and ftp://ftp.tummy.com/pub/python-memcached/
    
    IMPORTANT: data stuck into memcached is not guaranteed to be stable.
    It may disappear at any time, according to an internal LRU algorithm.
    In particular, you should be aware that the LRU algorithm is itself
    partitioned by object size (into "slabs"), so that a newer object
    may be removed before an older one if they are of significantly
    different sizes.
    
    Options:
        memcached.servers: a list of strings of the form 'IP-address:port'.
            These will be passed directly into the memcache.Client instance.
        
        memcached.indexed: if True (the default), this store will maintain
            an index of all stored objects in memcached itself. This is the
            'safe' choice, and necessary if your only store is memcached.
            If you run this store as an ObjectCache.cache, however, you
            should turn this off, allowing ObjectCache.nextstore to maintain
            the indexes--this allows the cache to run orders of magnitude
            faster.
    """
    
    def __init__(self, allOptions={}):
        storage.StorageManager.__init__(self, allOptions)
        
        self.name = allOptions['name']
        self.indexed = allOptions.pop("memcached.indexed", True)
        
        cache_opts = dict([(k[10:], v) for k, v in allOptions.iteritems()
                           if k.startswith("memcached.")])
        self.client = memcache.Client(**cache_opts)
        for s in self.client.servers:
            s.log = self.log_io
        
        self._keyattrs = {}
        
        # Populate this with {cls: float} pairs to use timeouts on set/add
        self.cache_timeouts = {}
    
    def connect_all(self):
        """Connect to all servers immediately (use this to warn on startup)."""
        for server in self.client.servers:
            server.acquire()
            try:
                if not server.assert_socket():
                    warnings.warn("Could not open a connection to memcached "
                                  "server %s" % server, errors.StorageWarning)
            finally:
                server.release()
    
    def log_io(self, msg):
        """I/O logging function."""
        if self.logflags & logflags.IO:
            self.log(logflags.IO.message("[%s] %s" % (thread.get_ident(), msg)))
    
    def hash(self, object):
        """Return a consistent hash for object (for use in a memcached key)."""
        # TODO: can we add overflow support for collisions?
        return md5.new(repr(object)).hexdigest()
    
    def _index_key(self, cls):
        """Return the key for the cached index of the given class."""
        return "%s:%s(%s)" % (self.name, cls.__name__,
                              ",".join(self._keyattrs[cls]))
    
    def register(self, cls):
        """Assert that Units of class 'cls' will be handled."""
        self._keyattrs[cls] = tuple(cls.identifiers or cls.properties)
        storage.StorageManager.register(self, cls)
    
    def _unit_key(self, unit):
        """Return the memcached key for the given unit."""
        cls = unit.__class__
        ident = tuple([getattr(unit, name) for name in self._keyattrs[cls]])
        return "%s:%s:%s" % (self.name, cls.__name__, self.hash(ident))
    
    def unit(self, cls, **kwargs):
        """A single Unit which matches the given kwargs, else None.
        
        The first Unit matching the kwargs is returned; if no Units match,
        None is returned.
        """
        filters = set(kwargs.keys())
        keyattrs = self._keyattrs[cls]
        if filters >= set(keyattrs):
            # Looking up a Unit by its identifiers.
            # Skip grabbing the cached class index (a HUGE optimization).
            key = tuple([kwargs[k] for k in keyattrs])
            key = "%s:%s:%s" % (self.name, cls.__name__, self.hash(key))
            unit = self.client.get(key)
            if unit is not None:
                matching = True
                if filters > set(keyattrs):
                    # We retrieved the Unit using a subset of the filters.
                    # Filter in full now.
                    for k, v in kwargs.iteritems():
                        if getattr(unit, k) != v:
                            matching = False
                            break
                
                if matching:
                    unit.cleanse()
                    if self.logflags & logflags.RECALL:
                        self.log(logflags.RECALL.message(cls, ('HIT', kwargs)))
                    return unit
            
            if self.logflags & logflags.RECALL:
                self.log(logflags.RECALL.message(cls, ('MISS', kwargs)))
            return None
        
        if self.indexed:
            if self.logflags & logflags.RECALL:
                self.log(logflags.RECALL.message(cls, ('INDEX', kwargs)))
            
            ci = self.client.get(self._index_key(cls)) or set()
            try:
                expr = logic.filter(**kwargs)
                return self._xrecall_inner(ci, expr).next()[0]
            except StopIteration:
                return None
        else:
            if self.logflags & logflags.RECALL:
                self.log(logflags.RECALL.message(cls, ('DEFER', kwargs)))
            return None
    
    def xrecall(self, classes, expr=None, order=None, limit=None, offset=None):
        """Yield units of the given cls which match the given expr."""
        if not self.indexed:
            return iter([])
        
        if isinstance(classes, dejavu.UnitJoin):
            return self._xmultirecall(classes, expr, order=order,
                                      limit=limit, offset=offset)
        
        cls = classes
        if self.logflags & logflags.RECALL:
            self.log(logflags.RECALL.message(cls, expr))
        
        ci = self.client.get(self._index_key(cls)) or set()
        if ci:
            data = self._xrecall_inner(ci, expr)
            return self._paginate(data, order, limit, offset, single=True)
        else:
            return iter([])
    
    def _xrecall_inner(self, keys, expr=None):
        """Private helper for self.xrecall."""
        units = self.client.get_multi(keys)
        # Iterate over the keys in the same order we were given.
        for key in keys:
            unit = units.get(key, None)
            if unit is not None and expr is None or expr(unit):
                unit.cleanse()
                # Must yield a sequence for use in _paginate.
                yield (unit,)
    
    def save(self, unit, forceSave=False):
        """Store the unit."""
        if self.logflags & logflags.SAVE:
            self.log(logflags.SAVE.message(unit, forceSave))
        
        if forceSave or unit.dirty():
            # Cleanse first because pickle state
            # includes _initial_property_hash.
            unit.cleanse()
            
            cls = unit.__class__
            key = self._unit_key(unit)
            timeout = self.cache_timeouts.get(cls, 0)
            self.client.set(key, unit, time=timeout)
            
            if self.indexed:
                ci = self.client.get(self._index_key(cls)) or set()
                ci.add(key)
                self.client.set(self._index_key(cls), ci)
    
    def destroy(self, unit):
        """Delete the unit."""
        if self.logflags & logflags.DESTROY:
            self.log(logflags.DESTROY.message(unit))
        
        key = self._unit_key(unit)
        self.client.delete(key)
        
        if self.indexed:
            cls = unit.__class__
            ci = self.client.get(self._index_key(cls)) or set()
            ci.discard(key)
            self.client.set(self._index_key(cls), ci)
    
    def reserve(self, unit):
        """Reserve storage space for the Unit."""
        if unit.identifiers:
            cls = unit.__class__
            timeout = self.cache_timeouts.get(cls, 0)
            if self.indexed:
                ci = self.client.get(self._index_key(cls)) or set()
                
                if not unit.sequencer.valid_id(unit.identity()):
                    ids = []
                    for key in ci:
                        otherunit = self.client.get(key)
                        if otherunit is not None:
                            ids.append(otherunit.identity())
                    unit.sequencer.assign(unit, ids)
                unit.cleanse()
                
                key = self._unit_key(unit)
                self.client.add(key, unit, time=timeout)
                
                ci.add(key)
                self.client.set(self._index_key(cls), ci)
            else:
                if not unit.sequencer.valid_id(unit.identity()):
                    raise NotImplementedError(
                        "Unindexed memcache cannot generate identifiers.")
                
                unit.cleanse()
                key = self._unit_key(unit)
                try:
                    self.client.add(key, unit, time=timeout)
                except IOError, exc:
                    if exc.args[0] == 'NOT_STORED':
                        pass
                    raise
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
        self.client.disconnect_all()
    
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
        for cls in self.classes:
            self.flush(cls)
    
    def create_storage(self, cls, conflicts='error'):
        """Create internal structures for the given class.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("create storage %s" % cls))
        
        try:
            self.client.add(self._index_key(cls), [])
        except IOError, exc:
            if exc.args[0] == 'NOT STORED':
                errors.conflict(conflicts, "Class %r already has storage."
                                % cls)
            else:
                raise
    
    def has_storage(self, cls):
        """If storage structures exist for the given class, return True."""
        return True
    
    def drop_storage(self, cls, conflicts='error'):
        """Destroy internal structures for the given class.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop storage %s" % cls))
        self.flush(cls)
    
    def add_property(self, cls, name, conflicts='error'):
        """Add internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("add property %s %s" % (cls, name)))
        
        if self.indexed:
            timeout = self.cache_timeouts.get(cls, 0)
            ci = self.client.get(self._index_key(cls)) or []
            for key in ci:
                unit = self.client.get(key)
                if unit is not None:
                    unit._properties[name] = None
                    unit.cleanse()
                    self.client.set(self._unit_key(unit), unit, time=timeout)
    
    def has_property(self, cls, name):
        """If storage structures exist for the given property, return True."""
        if self.indexed:
            ci = self.client.get(self._index_key(cls))
            
            if not ci:
                # We don't have any items, so there's nothing to
                # declare as 'unprepared'.
                return True
            
            for key in ci:
                unit = self.client.get(key)
                if unit is not None:
                    return name in unit._properties
        
        return True
    
    def drop_property(self, cls, name, conflicts='error'):
        """Destroy internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop property %s %s" % (cls, name)))
        
        if self.indexed:
            timeout = self.cache_timeouts.get(cls, 0)
            ci = self.client.get(self._index_key(cls)) or []
            for key in ci:
                unit = self.client.get(key)
                if unit is not None:
                    del unit._properties[name]
                    unit.cleanse()
                    self.client.set(self._unit_key(unit), unit, time=timeout)
    
    def rename_property(self, cls, oldname, newname, conflicts='error'):
        """Rename internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("rename property %s from %s to %s"
                                          % (cls, oldname, newname)))
        
        if self.indexed:
            timeout = self.cache_timeouts.get(cls, 0)
            ci = self.client.get(self._index_key(cls)) or []
            for key in ci:
                unit = self.client.get(key)
                if unit is not None:
                    unit._properties[newname] = unit._properties[oldname]
                    del unit._properties[oldname]
                    unit.cleanse()
                    self.client.set(self._unit_key(unit), unit, time=timeout)
    
    
    #                   Extra methods for use as a cache                   #
    
    def cachelen(self, cls):
        if self.indexed:
            return len(self.client.get(self._index_key(cls)))
        else:
            return 0
    
    def cached_units(self, cls):
        units = []
        if self.indexed:
            for key in self.client.get(self._index_key(cls)):
                unit = self.client.get(key)
                if unit is not None:
                    units.append(unit)
        return units
    
    def flush(self, cls):
        """Dump all objects of the given class."""
        if self.indexed:
            # Delete all units in the class index.
            for key in self.client.get(self._index_key(cls)) or []:
                self.client.delete(key)
            
            # Delete the class index.
            self.client.delete(self._index_key(cls))
        # TODO:
        # else:
        #     self.increment_generation(cls)

