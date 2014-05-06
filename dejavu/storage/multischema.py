"""A database Storage Manager which mixes multiple schemas.

This SM depends on each Dejavu class in the model possessing
an additional '_dbschema_name' attribute.
"""

import threading

import geniusql
import dejavu
from dejavu import storage, logflags, xray
from dejavu.storage import db
from dejavu.errors import conflict


# --------------------------- Storage Manager --------------------------- #


class MultiSchemaStorageManagerDB(db.StorageManagerDB):
    """StoreManager base class to save and retrieve Units using a DB."""
    
    def __init__(self, allOptions={}):
        storage.StorageManager.__init__(self, allOptions)
        self.reserve_lock = threading.Lock()
        
        # Config Overrides
        def get_option(name):
            item = allOptions.get(name)
            if isinstance(item, basestring):
                item = xray.classes(item)
            return item
        
        dbclass = get_option('Database Class')
        if dbclass:
            self.databaseclass = dbclass
        
        allOptions = dict([(str(k), v) for k, v in allOptions.iteritems()])
        
        self.db = self.databaseclass(**allOptions)
        
        self.schemas = {}
        schema_names = allOptions.get('Schemas', '')
        if isinstance(schema_names, basestring):
            schema_names = schema_names.split(', ')
        for name in schema_names:
            name = name.strip()
            if name:
                self.schemas[name] = self.db.schema(name)
        
        self._table_map = {}
        
        def logger(msg):
            if self.logflags & logflags.SQL:
                self.log(logflags.SQL.message(msg))
        self.db.log = logger
    
    def _seq_UnitSequencerDynamic(self, unit):
        """Reserve a unit (using the table's autoincrement fields)."""
        # Grab the new ID. This is threadsafe because reserve has a mutex.
        newids = self._table_map[unit.__class__].insert(**unit._properties)
        for k, v in newids.iteritems():
            setattr(unit, k, v)
    _seq_UnitSequencerInteger = _seq_UnitSequencerDynamic
    
    def _manual_reserve(self, unit):
        """Use when the DB cannot automatically generate an identifier.
        The identifiers will be supplied by UnitSequencer.assign().
        """
        t = self._table_map[unit.__class__]
        if not unit.sequencer.valid_id(unit.identity()):
            # Examine all existing IDs and grant the "next" one.
            data = list(self.db.select((t, unit.identifiers)))
            unit.sequencer.assign(unit, data)
        t.insert(**unit._properties)
    
    def save(self, unit, forceSave=False):
        """Update storage from unit's data (if unit.dirty())."""
        if self.logflags & logflags.SAVE:
            self.log(logflags.SAVE.message(unit, forceSave))
        
        if forceSave or unit.dirty():
            self._table_map[unit.__class__].save(**unit._properties)
            unit.cleanse()
    
    def destroy(self, unit):
        """Delete the unit."""
        if self.logflags & logflags.DESTROY:
            self.log(logflags.DESTROY.message(unit))
        self._table_map[unit.__class__].delete(**unit._properties)
    
    
    #                                Views                                #
    
    def tablejoin(self, join):
        """Return a geniusql Join tree for the given UnitJoin."""
        t1, t2 = join.class1, join.class2
        
        if isinstance(t1, dejavu.UnitJoin):
            wt1 = self.tablejoin(t1)
        else:
            wt1 = self._table_map[t1]
        
        if isinstance(t2, dejavu.UnitJoin):
            wt2 = self.tablejoin(t2)
        else:
            wt2 = self._table_map[t2]
        
        uj = geniusql.Join(wt1, wt2, join.leftbiased)
        # if the original UnitJoin had a custom association path,
        # copy it to the new Join instance
        uj.path = join.path
        return uj
    
    def _geniusql_query(self, query):
        """Return a Geniusql Query object for the given Dejavu Query."""
        rel = query.relation
        if isinstance(rel, dejavu.UnitJoin):
            rel = self.tablejoin(rel)
        else:
            rel = self._table_map[rel]
        return geniusql.Query(rel, query.attributes, query.restriction)
    
    def insert_into(self, name, query, distinct=False):
        """INSERT matching data INTO a new class and return the class."""
        if not isinstance(query, dejavu.Query):
            query = dejavu.Query(*query)
        
        self.db.insert_into(name, self._geniusql_query(query),
                            distinct=distinct)
        if isinstance(query.relation, dejavu.UnitJoin):
            for cls in query.relation:
                schema = self._table_map[cls].schema
                break
        else:
            schema = self._table_map[query.relation].schema
        return Modeler(schema).make_class(name)
    
    def make_class(self, name):
        """Return a (new) Unit class for the given storage name."""
        # TODO:
        raise NotImplementedError
    
    
    #                               Schemas                               #
    
    def create_database(self, conflicts='error'):
        """Create internal structures for the entire database.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("create database"))
        try:
            self.db.create()
            for s in self.schemas.itervalues():
                s.create()
        except geniusql.errors.MappingError, x:
            conflict(conflicts, str(x))
    
    def drop_database(self, conflicts='error'):
        """Destroy internal structures for the entire database.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop database"))
        
        try:
            self.db.drop()
        except geniusql.errors.MappingError, x:
            conflict(conflicts, str(x))
    
    def _make_table(self, cls):
        """Create and return a Table or View object for the given class."""
        s = self.schemas[cls._dbschema_name]
        if hasattr(cls, "view_statement"):
            gs = self._geniusql_statement(cls.view_statement)
            self._table_map[cls] = t = s.view(cls.__name__, gs)
            fields = []
            for key in cls.properties:
                t[key] = self._make_column(cls, key)
        else:
            self._table_map[cls] = t = s.table(cls.__name__)
            indices = cls.indices()
            fields = []
            for key in cls.properties:
                t[key] = col = self._make_column(cls, key)
                if key in indices:
                    t.add_index(key)
                if col.autoincrement and col.sequence_name is None:
                    # Not every database needs/uses sequence_name
                    col.sequence_name = s.sequence_name(t.name, key)
        
        # Copy associations to table.references.
        for k, v in cls._associations.iteritems():
            t.references[k] = (v.nearKey, v.farClass.__name__, v.farKey)
        
        return t
    
    def create_storage(self, cls, conflicts='error'):
        """Create internal structures for the given class.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("create storage %s" % cls))
        try:
            # Attach to a schema, which should call CREATE TABLE.
            s = self.schemas[cls._dbschema_name]
            s[cls.__name__] = self._make_table(cls)
        except geniusql.errors.MappingError, x:
            conflict(conflicts, str(x))
    
    def _make_column(self, cls, key):
        s = self._table_map[cls].schema
        prop = getattr(cls, key)
        col = s.column(prop.type, default=prop.default, hints=prop.hints)
        if key in cls.identifiers:
            col.key = True
            if isinstance(cls.sequencer, dejavu.UnitSequencerInteger):
                col.autoincrement = True
                col.initial = cls.sequencer.initial
        return col
    
    def has_storage(self, cls):
        """If storage structures exist for the given class, return True."""
        return cls.__name__ in self.schemas[cls._dbschema_name]
    
    def drop_storage(self, cls, conflicts='error'):
        """Destroy internal structures for the given class.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop storage %s" % cls))
        try:
            s = self._table_map[cls].schema
            del s[cls.__name__]
        except geniusql.errors.MappingError, x:
            conflict(conflicts, str(x))
    
    def rename_storage(self, oldname, newname, conflicts='error'):
        """Rename internal structures for the given class.
        
        conflicts: see errors.conflict.
        """
        # TODO:
        return NotImplementedError
    
    def add_property(self, cls, name, conflicts='error'):
        """Add internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("add property %s %s" %
                                          (cls, name)))
        if not self.has_property(cls, name):
            try:
                s = self._table_map[cls].schema
                t = s[cls.__name__]
                t[name] = col = self._make_column(cls, name)
                if col.autoincrement and col.sequence_name is None:
                    # Not every database needs/uses sequence_name
                    col.sequence_name = s.sequence_name(t.name, name)
            except geniusql.errors.MappingError, x:
                conflict(conflicts, str(x))
    
    def has_property(self, cls, name):
        """If storage structures exist for the given property, return True."""
        return name in self._table_map[cls][cls.__name__]
    
    def drop_property(self, cls, name, conflicts='error'):
        """Destroy internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop property %s %s" %
                                          (cls, name)))
        if self.has_property(cls, name):
            try:
                del self._table_map[cls][name]
            except geniusql.errors.MappingError, x:
                conflict(conflicts, str(x))
    
    def rename_property(self, cls, oldname, newname, conflicts='error'):
        """Rename internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message(
                "rename property %s from %s to %s" %
                (cls, oldname, newname)))
        
        try:
            t = self._table_map[cls]
            s = t.schema
            
            # Sometimes, a Dejavu Schema will change a code model first, and
            # then change the database afterward. So it's possible that the
            # column we're trying to rename hasn't been loaded, because the
            # model layer no longer references it. So if table[oldname]
            # raises a KeyError, try to find a column that matches oldkey.
            tempcol = None
            try:
                t[oldname]
            except KeyError:
                c = [x for x in s._get_columns(t.name)
                     if x.name == s.column_name(t.name, oldname)]
                if not c:
                    raise KeyError("Rename failed. Old column %r not found in %r."
                                   % (oldname, t.name))
                oldcol = c[0]
                # Use the superclass call to avoid DROP COLUMN/ADD COLUMN.
                dict.__setitem__(t, oldname, oldcol)
            
            t.rename(oldname, newname)
        except geniusql.errors.MappingError, x:
            conflict(conflicts, str(x))
    
    def add_index(self, cls, name, conflicts='error'):
        """Add an index on the given property.
        
        conflicts: see errors.conflict.
        """
        try:
            self._table_map[cls].add_index(name)
        except geniusql.errors.MappingError, x:
            conflict(conflicts, str(x))
    
    def has_index(self, cls, name):
        """If an index exists on the given property, return True."""
        return name in self._table_map[cls].indices
    
    def drop_index(self, cls, name, conflicts='error'):
        """Destroy any index on the given property.
        
        conflicts: see errors.conflict.
        """
        try:
            del self._table_map[cls].indices[name]
        except geniusql.errors.MappingError, x:
            conflict(conflicts, str(x))
    
    auto_discover = True
    
    def map(self, classes, conflicts='error'):
        """Map classes to internal storage.
        
        If self.auto_discover is True (the default), then Table/Column/Index
        objects will be formed by inspecting the underlying database using
        self.sync().
        
        If auto_discover is False, then mock Table/Column/Index objects
        will be used instead; this provides a performance improvement
        in scenarios where the model maps perfectly to the database
        and changes to the database are not expected outside the model.
        
        conflicts: see errors.conflict.
        """
        # Map tables before views, because views depend on them
        tables = [cls for cls in classes if not hasattr(cls, 'view_statement')]
        views = [cls for cls in classes if hasattr(cls, 'view_statement')]
        classes = tables + views
        
        if self.auto_discover:
            self.sync(classes, conflicts)
        else:
            for cls in classes:
                s = self.schemas[cls._dbschema_name]
                if cls.__name__ in s:
                    # If our consumer-side key is already present, skip this cls.
                    # This allows callers to auto-sync class by class
                    # without making a new Table object each time.
                    continue
                
                t = self._make_table(cls)
                
                # Use the superclass call to avoid DROP/CREATE TABLE
                dict.__setitem__(s, cls.__name__, t)
    
    def sync(self, classes, conflicts='error'):
        """Map classes to existing Table objects (found via discovery).
        
        conflicts: see errors.conflict.
        """
        for cls in classes:
            clsname = cls.__name__
            
            s = self.schemas[cls._dbschema_name]
            if clsname in s:
                # If our consumer-side key is already present, skip this cls.
                # This allows callers to auto-sync class by class
                # without calling the expensive discover() func each time.
                continue
            
            self._table_map[cls] = self._find_table(s, cls, conflicts)

