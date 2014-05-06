try:
    from bsddb._db import DBNoSuchFileError
except ImportError:
    DBNoSuchFileError = object()

import anydbm
import os

try:
    import cPickle as pickle
except ImportError:
    import pickle

import shelve
import threading

import dejavu
from dejavu import errors, logic, logflags, storage


class StorageManagerShelve(storage.StorageManager):
    """StoreManager to save and retrieve Units via stdlib shelve."""
    
    def __init__(self, allOptions={}):
        storage.StorageManager.__init__(self, allOptions)
        
        self.recall_stride = allOptions.get('recall_stride', 100)
        
        path = allOptions['Path']
        if not os.path.isabs(path):
            path = os.path.join(os.getcwd(), path)
        if not os.path.exists(path):
            raise IOError(2, "No such directory: '%s'" % path)
        self.shelvepath = path
        
        # A dictionary whose keys are classes and whose
        # values are objects returned by shelve.open().
        # Those values are dict-like objects with keys of type 'str'.
        self.shelves = {}
        
        self.locks = {}
    
    def shutdown(self, conflicts='error'):
        """Shut down all connections to internal storage.
        
        conflicts: see errors.conflict.
        """
        while self.shelves:
            cls, shelf = self.shelves.popitem()
            lock = self.get_lock(cls)
            try:
                shelf.close()
            finally:
                lock.release()
        self.locks = {}
    
    def unit(self, cls, **kwargs):
        """A single Unit which matches the given kwargs, else None.
        
        The first Unit matching the kwargs is returned; if no Units match,
        None is returned.
        """
        if self.logflags & logflags.RECALL:
            self.log(logflags.RECALL.message(cls, kwargs))
        
        if set(kwargs.keys()) == set(cls.identifiers):
            # Looking up a Unit by its identifiers.
            # Skip grabbing the entire shelf (a HUGE optimization).
            key = self.key(tuple([kwargs[k] for k in cls.identifiers]))
            
            lock = self.get_lock(cls)
            try:
                data = self.shelves[cls] or {}
                unitdict = data.get(key, None)
            finally:
                lock.release()
            
            if unitdict is None:
                return None
            else:
                # Set props directly to avoid __set__ and default overhead.
                unit = cls.__new__(cls)
                unit._zombie = True
                unit.__init__()
                unit._properties = unitdict
                unit.cleanse()
                return unit
        
        lock = self.get_lock(cls)
        try:
            data = self.shelves[cls] or {}
            keys = data.keys()
        finally:
            lock.release()
        
        try:
            expr = logic.filter(**kwargs)
            return self._xrecall_inner(cls, expr, keys).next()[0]
        except StopIteration:
            return None
    
    def xrecall(self, classes, expr=None, order=None, limit=None, offset=None):
        """Yield units of the given cls which match the given expr."""
        if isinstance(classes, dejavu.UnitJoin):
            return self._xmultirecall(classes, expr, order=order,
                                      limit=limit, offset=offset)
        
        cls = classes
        if self.logflags & logflags.RECALL:
            self.log(logflags.RECALL.message(cls, expr))
        
        lock = self.get_lock(cls)
        try:
            data = self.shelves[cls] or {}
            keys = data.keys()
        finally:
            lock.release()
        
        if not isinstance(expr, logic.Expression):
            expr = logic.Expression(expr)
        
        inner = self._xrecall_inner(cls, expr, keys)
        return self._paginate(inner, order, limit, offset, single=True)
    
    def _xrecall_inner(self, cls, expr, keys):
        """Yield units which match the expr."""
        stride = self.recall_stride
        for cursor in xrange(0, len(keys), stride):
            keyset = keys[cursor:cursor+stride]
            for unit in self._xrecall_inner_inner(cls, keyset):
                if expr is None or expr(unit):
                    unit.cleanse()
                    # Must yield a sequence for use in _paginate.
                    yield (unit,)
    
    def _xrecall_inner_inner(self, cls, keyset):
        """Grab a chunk of units."""
        units = []
        lock = self.get_lock(cls)
        try:
            data = self.shelves[cls]
            if data:
                for key in keyset:
                    unitdict = data.get(key, None)
                    if unitdict is not None:
                        # Set props directly to avoid __set__ and default overhead.
                        unit = cls.__new__(cls)
                        unit._zombie = True
                        unit.__init__()
                        unit._properties = unitdict
                        units.append(unit)
        finally:
            lock.release()
        return units
    
    def key(self, arg):
        return pickle.dumps(arg)
    
    def reserve(self, unit):
        """Reserve a persistent slot for unit."""
        if unit.identifiers:
            cls = unit.__class__
            lock = self.get_lock(cls)
            try:
                data = self.shelves[cls]
                if not unit.sequencer.valid_id(unit.identity()):
                    ids = [[row[key] for key in unit.identifiers]
                           for row in data.itervalues()]
                    unit.sequencer.assign(unit, ids)
                data[self.key(unit.identity())] = unit._properties
                unit.cleanse()
            finally:
                lock.release()
        else:
            # This class has no identifiers, so skip reserve and wait for save.
            pass
        
        # Usually we log ASAP, but here we log after
        # the unit has had a chance to get an auto ID.
        if self.logflags & logflags.RESERVE:
            self.log(logflags.RESERVE.message(unit))
    
    def save(self, unit, forceSave=False):
        """Update storage from unit's data."""
        if self.logflags & logflags.SAVE:
            self.log(logflags.SAVE.message(unit, forceSave))
        
        if forceSave or unit.dirty():
            cls = unit.__class__
            lock = self.get_lock(cls)
            try:
                data = self.shelves[cls]
                if unit.identifiers:
                    key = self.key(unit.identity())
                else:
                    # This class has no identifiers, so hash the whole dict.
                    key = self.key(unit._properties)
                # Replace the entire value to get around writeback issues.
                # See the docs on "shelve" for more info.
                data[key] = unit._properties
                unit.cleanse()
            finally:
                lock.release()
    
    def destroy(self, unit):
        """Delete the unit."""
        if self.logflags & logflags.DESTROY:
            self.log(logflags.DESTROY.message(unit))
        
        cls = unit.__class__
        lock = self.get_lock(cls)
        try:
            data = self.shelves[cls]
            if unit.identifiers:
                del data[self.key(unit.identity())]
            else:
                # This class has no identifiers, so hash the whole dict.
                del data[self.key(unit._properties)]
        finally:
            lock.release()
    
    def version(self):
        import sys
        return "Shelve version: %s" % sys.version
    
    ext = ".djv"
    
    def filename(self, cls):
        """Return the full path for the given class."""
        return os.path.join(self.shelvepath, cls.__name__ + self.ext)
    
    def insert_into(self, name, query, distinct=False):
        """INSERT matching data INTO a new class and return the class."""
        if not isinstance(query, dejavu.Query):
            query = dejavu.Query(*query)
        
        newclass = self.make_class(name)
        self._create_named_storage(newclass)
        
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("create storage %r" % newcls))
        
        source = self.xview(query, distinct)
        for row in source:
            data = self.shelves[newclass]
            if newclass.identifiers:
                key = unit.identity()
            else:
                key = hash(unit._properties)
            data[key] = dict([(k, v) for k, v in zip(source.descr, row)])
        
        return newclass
    
    #                               Schemas                               #
    
    def create_database(self, conflicts='error'):
        """Create internal structures for the entire database.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("create database"))
        
        try:
            if not self.has_database():
                os.makedirs(self.shelvepath)
        except Exception, x:
            errors.conflict(conflicts, str(x))
    
    def has_database(self):
        """If storage exists for this database, return True."""
        return os.path.exists(self.shelvepath)
    
    def drop_database(self, conflicts='error'):
        """Destroy internal structures for the entire database.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop database"))
        
        while self.shelves:
            cls, shelf = self.shelves.popitem()
            shelf.close()
        
        for name in os.listdir(self.shelvepath):
            name = os.path.join(self.shelvepath, name)
            if not os.path.isdir(name) and name.endswith(self.ext):
                try:
                    os.remove(name)
                except Exception, x:
                    errors.conflict(conflicts, str(x))
    
    def create_storage(self, cls, conflicts='error'):
        """Create internal structures for the given class.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("create storage %r" % cls))
        try:
            self._create_named_storage(cls)
        except anydbm.error, x:
            errors.conflict(conflicts, str(x))
    
    def get_lock(self, cls):
        if cls not in self.locks:
            lock = self.locks[cls] = threading.Lock()
        else:
            lock = self.locks[cls]
        lock.acquire()
        return lock
    
    def _create_named_storage(self, cls):
        lock = self.get_lock(cls)
        try:
            s = shelve.open(self.filename(cls), 'n')
        finally:
            lock.release()
    
    def has_storage(self, cls):
        """If storage structures exist for the given class, return True."""
        return os.path.exists(self.filename(cls))
    
    def drop_storage(self, cls, conflicts='error'):
        """Destroy internal structures for the given class.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop storage %r" % cls))
        
        lock = self.get_lock(cls)
        try:
            try:
                shelf = self.shelves.pop(cls)
            except KeyError:
                pass
            else:
                shelf.close()
            
            try:
                os.remove(self.filename(cls))
            except Exception, x:
                errors.conflict(conflicts, str(x))
        finally:
            lock.release()
    
    def add_property(self, cls, name, conflicts='error'):
        """Create internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("add property %r %r" % (cls, name)))
        
        lock = self.get_lock(cls)
        try:
            try:
                data = self.shelves[cls]
            except KeyError, x:
                errors.conflict(conflicts, str(x))
            
            for id, props in data.items():
                props[name] = None
                data[id] = props
        finally:
            lock.release()
    
    def drop_property(self, cls, name, conflicts='error'):
        """Destroy internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop property %r %r" % (cls, name)))
        
        lock = self.get_lock(cls)
        try:
            try:
                data = self.shelves[cls]
            except KeyError, x:
                errors.conflict(conflicts, str(x))
            
            for id, props in data.items():
                del props[name]
                data[id] = props
        finally:
            lock.release()
    
    def rename_property(self, cls, oldname, newname, conflicts='error'):
        """Rename internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("rename property %r from %r to %r"
                                          % (cls, oldname, newname)))
        
        lock = self.get_lock(cls)
        try:
            try:
                data = self.shelves[cls]
            except KeyError, x:
                errors.conflict(conflicts, str(x))
            
            for id, props in data.items():
                props[newname] = props[oldname]
                del props[oldname]
                data[id] = props
        finally:
            lock.release()
    
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
            
            s = self.shelves.get(cls)
            if s is None:
                try:
                    s = shelve.open(self.filename(cls), 'w')
                except anydbm.error, x:
                    errors.conflict(conflicts, str(x))
                else:
                    self.shelves[cls] = s

