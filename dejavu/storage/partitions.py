"""A StorageManager for Dejavu which mediates multiple stores."""

try:
    # Builtin in Python 2.4+
    set
except NameError:
    # Module in Python 2.3
    from sets import Set as set

from geniusql import logic

import dejavu
from dejavu import errors, storage, logflags


class VerticalPartitioner(storage.StorageManager):
    """A mediator for multiple vertically-partitioned stores."""
    
    __metaclass__ = dejavu._AttributeDocstrings
    
    stores = {}
    stores__doc = "A map from store names to StorageManager instances."
    
    classmap = {}
    classmap__doc = """
    A map from Unit classes to lists of StorageManager instances.
    
    DDL methods will generally dispatch to all stores for each class.
    DML methods will generally dispatch to classmap[unit.__class__][0];
    those which involve multiple classes (e.g. multirecall), will try
    to find a single store which handles all classes in the given
    relation. To override this default search, you can add entries
    to classmap of the form: {(clsA, clsB, clsC): [store1]},
    which instructs the partitioner to use the given store for
    any Join with the same order, such as (clsA << clsB) & clsC."""
    
    def __init__(self, allOptions={}):
        storage.StorageManager.__init__(self, allOptions)
        self.stores = {}
        self.classmap = {}
    
    def migrate(self, classes, new_store, old_store=None, copy_only=False):
        """Move all units of the given class(es) to new_store.
        
        copy_only: if False (the default), this copies the data to the new
            store, deletes it from the old store, and updates self.classmap.
            If True, the data is copied to the new store only.
        """
        if not isinstance(classes, (list, tuple)):
            classes = list(classes)
        
        for cls in classes:
            new_store.classes.add(cls)
            if not new_store.has_storage(cls):
                new_store.create_storage(cls)
            
            if old_store is None:
                units = self.xrecall(cls)
            else:
                units = old_store.xrecall(cls)
            
            for unit in units:
                new_store.reserve(unit)
                new_store.save(unit, forceSave=True)
                if not copy_only:
                    self.destroy(unit)
            
            if not copy_only:
                classmap = self.classmap[cls]
                if old_store is None:
                    for store in classmap:
                        store.classes.remove(cls)
                        classmap.remove(store)
                else:
                    old_store.classes.remove(cls)
                    if old_store in classmap:
                        classmap.remove(old_store)
                
                if new_store not in classmap:
                    classmap.append(new_store)
    
    def migrate_all(self, new_store, old_store=None, copy_only=False):
        """Copy all units (of old_store) to new_store."""
        if old_store is None:
            for store in self.classmap[cls]:
                self.migrate(store.classes, new_store, store, copy_only)
        else:
            self.migrate(store.classes, new_store, old_store, copy_only)
    
    def add_store(self, name, store):
        """Register a StorageManager to be mediated.
        
        name: a key for the given store.
        store: a StorageManager instance. The given store should have its
            'classes' attribute already set (or you will have to populate
            self.classmap manually).
        """
        self.stores[name] = store
        for cls in store.classes:
            self.classmap.setdefault(cls, []).insert(0, store)
            self.register(cls)
        return store
    
    def remove_store(self, name):
        """Remove (unregister) the named store.
        
        All classes associated to the given store will be disassociated.
        """
        if name in self.stores:
            store = self.stores[name]
            
            # Disassociate all registered classes with this store.
            for cls, stores in self.classmap.items():
                if store in stores:
                    stores.remove(store)
                    if not stores:
                        del self.classmap[cls]
                        self.classes.remove(cls)
            
            del self.stores[name]
    
    def map(self, classes, conflicts='error'):
        """Map classes to internal storage.
        
        conflicts: see errors.conflict.
        """
        storemap = {}
        for cls in classes:
            for store in self.classmap[cls]:
                bucket = storemap.setdefault(store, [])
                bucket.append(cls)
        storemap = [(getattr(s, 'loadOrder', 5), s, c)
                    for s, c in storemap.iteritems()]
        storemap.sort()
        
        for order, store, classlist in storemap:
            try:
                store.map(classlist, conflicts=conflicts)
            except errors.MappingError, x:
                for key in self.stores:
                    if self.stores[key] is store:
                        break
                else:
                    key = None
                x.args += (key, store.__class__)
                raise
    
    def map_all(self, conflicts='error'):
        """Map all registered classes to internal storage structures.
        
        This method is idempotent, but that doesn't mean cheap. Try not
        to call it very often (once at app startup is usually enough).
        
        conflicts: see errors.conflict.
        """
        storemap = {}
        for cls, stores in self.classmap.iteritems():
            for store in stores:
                bucket = storemap.setdefault(store, [])
                bucket.append(cls)
        storemap = [(getattr(s, 'loadOrder', 5), s, c)
                    for s, c in storemap.iteritems()]
        storemap.sort()
        
        for order, store, classes in storemap:
            try:
                store.map(classes, conflicts=conflicts)
            except errors.MappingError, x:
                for key in self.stores:
                    if self.stores[key] is store:
                        break
                else:
                    key = None
                x.args += (key, store.__class__)
                raise
    
    def shutdown(self, conflicts='error'):
        """Shutdown self and all its stores.
        
        conflicts: see errors.conflict.
        """
        # Tell all stores to shut down.
        stores = [(getattr(v, 'shutdownOrder', 5), v, k) for k, v in self.stores.iteritems()]
        stores.sort()
        for order, store, name in stores:
            store.shutdown(conflicts=conflicts)
    
    def version(self):
        """Return provider-specific version strings for each mediated store."""
        output = []
        for store in self.stores.itervalues():
            if store.version:
                output.append(store.version())
        return '\n\n'.join(output)
    
    
    # --------------------- Unit Class Registration --------------------- #
    
    def create_database(self, conflicts='error'):
        for s in self.stores.itervalues():
            s.create_database(conflicts=conflicts)
    
    def has_database(self):
        """If storage exists for this database, return True."""
        for s in self.stores.itervalues():
            if not s.has_database():
                return False
        return True
    
    def drop_database(self, conflicts='error'):
        for s in self.stores.itervalues():
            s.drop_database(conflicts=conflicts)
    
    def create_storage(self, cls, conflicts='error'):
        """Create storage space for cls."""
        for store in self.classmap[cls]:
            store.create_storage(cls, conflicts=conflicts)
    
    def has_storage(self, cls):
        """If storage space for cls exists, return True (False otherwise)."""
        for store in self.classmap[cls]:
            if not store.has_storage(cls):
                return False
        return True
    
    def drop_storage(self, cls, conflicts='error'):
        """Remove storage space for cls."""
        for store in self.classmap[cls]:
            store.drop_storage(cls, conflicts=conflicts)
    
    def add_property(self, cls, name, conflicts='error'):
        """Add storage space for the named property of the given cls."""
        for store in self.classmap[cls]:
            store.add_property(cls, name, conflicts=conflicts)
    
    def has_property(self, cls, name):
        """If storage structures exist for the given property, return True."""
        for store in self.classmap[cls]:
            if not store.has_property(cls, name):
                return False
        return True
    
    def drop_property(self, cls, name, conflicts='error'):
        """Drop storage space for the named property of the given cls."""
        for store in self.classmap[cls]:
            store.drop_property(cls, name, conflicts=conflicts)
    
    def rename_property(self, cls, oldname, newname, conflicts='error'):
        """Rename storage space for the property of the given cls."""
        for store in self.classmap[cls]:
            store.rename_property(cls, oldname, newname, conflicts=conflicts)
    
    def add_index(self, cls, name, conflicts='error'):
        """Add an index to the given property.
        
        conflicts: see errors.conflict.
        """
        for store in self.classmap[cls]:
            store.add_index(cls, name, conflicts=conflicts)
    
    def has_index(self, cls, name):
        """If an index exists for the given property, return True."""
        for store in self.classmap[cls]:
            if not store.has_index(cls, name):
                return False
        return True
    
    def drop_index(self, cls, name, conflicts='error'):
        """Destroy any index on the given property.
        
        conflicts: see errors.conflict.
        """
        for store in self.classmap[cls]:
            store.drop_index(cls, name, conflicts=conflicts)
    
    
    # ------------------------------- DML ------------------------------- #
    
    def reserve(self, unit):
        """Reserve storage space for the Unit."""
        self.classmap[unit.__class__][0].reserve(unit)
    
    def save(self, unit, forceSave=False):
        """Store the unit's property values."""
        self.classmap[unit.__class__][0].save(unit, forceSave)
    
    def destroy(self, unit):
        """Delete the unit."""
        self.classmap[unit.__class__][0].destroy(unit)
    
    def unit(self, cls, **kwargs):
        return self.classmap[cls][0].unit(cls, **kwargs)
    
    def xrecall(self, classes, expr=None, order=None, limit=None, offset=None):
        """Yield a sequence of Unit instances which satisfy the expression."""
        if isinstance(classes, dejavu.UnitJoin):
            for unitrow in self._xmultirecall(classes, expr, order=order,
                                              limit=limit, offset=offset):
                yield unitrow
        else:
            store = self.classmap[classes][0]
            if self.logflags & logflags.RECALL:
                self.log(logflags.RECALL.message(classes, expr))
            for unit in store.xrecall(classes, expr, order, limit, offset):
                yield unit
    
    def _xmultirecall(self, classes, expr=None,
                      order=None, limit=None, offset=None):
        """Yield lists of units of the given classes which match expr.
        
        This does not yet handle multiple classes in disparate stores.
        """
        return self._single_store(classes)._xmultirecall(
            classes, expr, order=order, limit=limit, offset=offset)
    
    def _single_store(self, relation):
        """Return the store for the given relation (or raise ValueError)."""
        if hasattr(relation, "class1"):
            # This is a UnitJoin.
            try:
                # First, see if there's an explicit entry in self.classmap
                # for tuple([cls for cls in relation]).
                return self.classmap[tuple(relation)][0]
            except (KeyError, IndexError):
                # Otherwise, return the first store that handles all
                # classes in relation.
                stores = None
                for cls in relation:
                    if stores is None:
                        stores = set(self.classmap[cls])
                    else:
                        stores &= set(self.classmap[cls])
                
                for store in stores or []:
                    return store
                
                raise ValueError("This operation does not support multiple"
                                 " classes in disparate stores.")
        else:
            return self.classmap[relation][0]
    
    def xview(self, query, order=None, limit=None, offset=None, distinct=False):
        """Yield tuples of attribute values for the given query.
        
        Each yielded value will be a list of values, in the same order as
        the Query.attributes. This facilitates unpacking in iterative
        consumer code like:
        
        for id, name in store.view(Query(Invoice, ['ID', 'Name'], f)):
            print id, ": ", name
        
        This is generally much faster than recall, and should be preferred
        for performance-sensitive code.
        """
        if not isinstance(query, dejavu.Query):
            query = dejavu.Query(*query)
        
        if self.logflags & logflags.VIEW:
            self.log(logflags.VIEW.message(query, distinct))
        
        store = self._single_store(query.relation)
        for row in store.xview(query, order=order, limit=limit,
                               offset=offset, distinct=distinct):
            yield row
    
    def insert_into(self, name, query, distinct=False):
        """INSERT matching data INTO a new class and return the class."""
        if not isinstance(query, dejavu.Query):
            query = dejavu.Query(*query)
        
        store = self._single_store(query.relation)
        return store.insert_into(name, query, distinct)
    
    #                        Transaction Management                        #
    
    def start(self, isolation=None):
        """Start a transaction."""
        for store in self.stores.itervalues():
            # By default, stores do not support transactions,
            # in which case 'start' will be None.
            if store.start:
                store.start(isolation)
    
    def commit(self):
        """Commit the current transaction.
        
        If errors occur during this process, they are not trapped here.
        You must either call rollback yourself (or fix the problem and
        try to commit again).
        """
        for store in self.stores.itervalues():
            if store.commit:
                store.commit()
    
    def rollback(self):
        """Roll back the current transaction."""
        for store in self.stores.itervalues():
            if store.rollback:
                store.rollback()


