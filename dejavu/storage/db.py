"""Base classes and tools for writing database Storage Managers.

DATA TYPES
==========
Database Storage Manager modules are mostly adapters to support round-trip
data coercion:

Unit type -> [SQL repr ->] DB -> incoming Python value -> Unit type

Since Dejavu relies on external database servers for its persistence,
Python datatypes must be converted to column types in the DB. When writing
a StorageManager, you should make sure that your type conversions can handle
at least the following limitations: If possible, implement the type with no
limits. Also, follow UnitProperty.hints['bytes'] where possible. A value
of zero for hints['bytes'] implies no limit. If no value is given, try to
assume no limit, although you may choose whatever default size you wish
(255 is common for strings).

ENCODING ISSUES
===============
All SQL sent to the database must be strings, not unicode. You can set the
encoding of the Adapters (I may add a more centralized encoding context in
the future). We must use encoded strings so that we can mix encodings
within the same string; for example, we might have a DB which understands
utf8, but a pickle value which will be encoded in raw-unicode-escape inline
with that. All values, therefore, must be coerced before we try to join
them into an SQL statement string.

"""

import threading
import warnings


import geniusql
from geniusql import logic, logicfuncs

import dejavu
from dejavu import storage, logflags, xray
from dejavu.errors import StorageWarning, MappingError, conflict


# --------------------------- Storage Manager --------------------------- #


class StorageManagerDB(storage.StorageManager):
    """StoreManager base class to save and retrieve Units using a DB."""
    
    databaseclass = geniusql.Database
    
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
        self.schema = self.db.schema()
        
        if 'Prefix' in allOptions:
            self.schema.prefix = allOptions['Prefix']
        
        def logger(msg):
            if self.logflags & logflags.SQL:
                self.log(logflags.SQL.message(msg))
        self.db.log = logger
    
    def version(self):
        return self.db.version()
    
    def shutdown(self, conflicts='error'):
        """Shut down all connections to internal storage.
        
        conflicts: see errors.conflict.
        """
        self.db.connections.shutdown()
    
    def xrecall(self, classes, expr=None, order=None, limit=None, offset=None):
        """Yield a sequence of Unit instances which satisfy the expression."""
        if limit == 0:
            return
        if offset and not order:
            raise TypeError("Order argument expected when offset is provided.")
        
        if isinstance(classes, dejavu.UnitJoin):
            for unitrow in self._xmultirecall(classes, expr, order=order,
                                              limit=limit, offset=offset):
                yield unitrow
            return
        else:
            cls = classes
        
        if self.logflags & logflags.RECALL:
            self.log(logflags.RECALL.message(cls, expr))
        
        clsname = cls.__name__
        
        # Put the identifier properties first, in case other fields
        # depend upon them.
        idnames = list(cls.identifiers)
        attrs = idnames + [x for x in cls.properties if x not in idnames]
        coercers = [getattr(cls, key).coerce for key in attrs]
        
        data = self.select((cls, attrs, expr), order=order,
                           limit=limit, offset=offset)
        for row in data:
            unit = cls.__new__(cls)
            unit._zombie = True
            unit.__init__()
            
            for key, value, propcoerce in zip(attrs, row, coercers):
                try:
                    if propcoerce:
                        value = propcoerce(unit, value)
                    unit._properties[key] = value
                except UnicodeDecodeError, x:
                    x.reason += " [%r: %r]" % (key, value)
                    raise
                except Exception, x:
                    x.args += (key, value)
                    raise
            
            # If our SQL is imperfect, don't yield it to the
            # caller unless it passes expr(unit).
            if expr and data.statement.imperfect:
                if not expr(unit):
                    continue
            
            unit.cleanse()
            yield unit
    
    def reserve(self, unit):
        """Reserve a persistent slot for unit."""
        self.reserve_lock.acquire()
        try:
            # First, see if our db subclass has a handler that
            # uses the DB to generate the appropriate identifier(s).
            seqclass = unit.sequencer.__class__.__name__
            seq_handler = getattr(self, "_seq_%s" % seqclass, None)
            if seq_handler:
                seq_handler(unit)
            else:
                self._manual_reserve(unit)
            unit.cleanse()
        finally:
            self.reserve_lock.release()
        
        # Usually we log ASAP, but here we log after
        # the unit has had a chance to get an auto ID.
        if self.logflags & logflags.RESERVE:
            self.log(logflags.RESERVE.message(unit))
    
    def _seq_UnitSequencerDynamic(self, unit):
        """Reserve a unit (using the table's autoincrement fields)."""
        cls = unit.__class__
        
        # Grab the new ID. This is threadsafe because reserve has a mutex.
        newids = self.schema[cls.__name__].insert(**unit._properties)
        for k, v in newids.iteritems():
            setattr(unit, k, v)
    _seq_UnitSequencerInteger = _seq_UnitSequencerDynamic
    
    def _manual_reserve(self, unit):
        """Use when the DB cannot automatically generate an identifier.
        The identifiers will be supplied by UnitSequencer.assign().
        """
        cls = unit.__class__
        t = self.schema[cls.__name__]
        if not unit.sequencer.valid_id(unit.identity()):
            # Examine all existing IDs and grant the "next" one.
            data = list(self.db.select((t, cls.identifiers)))
            cls.sequencer.assign(unit, data)
        t.insert(**unit._properties)
    
    def save(self, unit, forceSave=False):
        """Update storage from unit's data (if unit.dirty())."""
        if self.logflags & logflags.SAVE:
            self.log(logflags.SAVE.message(unit, forceSave))
        
        if forceSave or unit.dirty():
            self.schema[unit.__class__.__name__].save(**unit._properties)
            unit.cleanse()
    
    def destroy(self, unit):
        """Delete the unit."""
        if self.logflags & logflags.DESTROY:
            self.log(logflags.DESTROY.message(unit))
        
        table = self.schema[unit.__class__.__name__]
        table.delete(**unit._properties)
    
    
    #                                Views                                #
    
    def tablejoin(self, join):
        """Return a geniusql Join tree for the given UnitJoin."""
        t1, t2 = join.class1, join.class2
        
        if isinstance(t1, dejavu.UnitJoin):
            wt1 = self.tablejoin(t1)
        else:
            wt1 = self.schema[t1.__name__]
        
        if isinstance(t2, dejavu.UnitJoin):
            wt2 = self.tablejoin(t2)
        else:
            wt2 = self.schema[t2.__name__]
        
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
        elif rel is None:
            # This is a Geniusql-ism: send the schema when we have no FROM.
            rel = self.schema
        else:
            rel = self.schema[rel.__name__]
        return geniusql.Query(rel, query.attributes, query.restriction)
    
    def _geniusql_statement(self, statement):
        """Return a Geniusql Statement object for the given Dejavu Statement."""
        return geniusql.Statement(
            self._geniusql_query(statement.query),
            order=statement.order, limit=statement.limit,
            offset=statement.offset, distinct=statement.distinct)
    
    def select(self, query, order=None, limit=None, offset=None, distinct=None):
        """Return a geniusql Dataset for the given Query object."""
        if not isinstance(query, dejavu.Query):
            query = dejavu.Query(*query)
        
        return self.db.select(self._geniusql_query(query),
                              order=order, limit=limit, offset=offset,
                              distinct=distinct, strict=False)
    
    def insert_into(self, name, query, distinct=False):
        """INSERT matching data INTO a new class and return the class."""
        if not isinstance(query, dejavu.Query):
            query = dejavu.Query(*query)
        
        self.db.insert_into(name, self._geniusql_query(query),
                            distinct=distinct)
        return Modeler(self.schema).make_class(name)
    
    def make_class(self, name):
        """Return a (new) Unit class for the given storage name."""
        return Modeler(self.schema).make_class(name)
    
    def xview(self, query, order=None, limit=None, offset=None, distinct=False):
        """Yield value tuples for the given query."""
        if not isinstance(query, dejavu.Query):
            query = dejavu.Query(*query)
        
        if self.logflags & logflags.VIEW:
            self.log(logflags.VIEW.message(query, distinct))
        
        data = self.select(query, order=order, limit=limit, offset=offset,
                           distinct=distinct)
        if data.statement.imperfect:
            # ^%$#@! There's no way to handle imperfect queries without
            # creating all involved Units, which defeats the performance
            # benefits of view.
            clsname = self.__class__.__name__
            warnings.warn("The requested query cannot produce perfect SQL "
                          "with a %s datasource. It may take an absurdly "
                          "long time to run, since each unit must be fully-"
                          "formed. %s" % (clsname, query), StorageWarning)
            for row in storage.StorageManager.xview(self, query, order=order,
                                                    limit=limit, offset=offset,
                                                    distinct=distinct):
                yield row
        else:
            # Use tuples for hashability
            for row in data:
                yield tuple(row)
    
    def count(self, cls, expr=None):
        """Number of Units of the given cls which match the given expr."""
        if cls.identifiers:
            uniq = cls.identifiers
        else:
            uniq = cls._properties.keys()
        # TODO: handle multiple args to count()
        counter = lambda x: [logicfuncs.count(getattr(x, uniq[0]))]
        
        query = dejavu.Query(cls, counter, expr)
        
        if self.logflags & logflags.VIEW:
            self.log(logflags.VIEW.message(query, False))
        
        data = self.select(query)
        if data.statement.imperfect:
            # ^%$#@! There's no way to handle imperfect queries without
            # creating all involved Units, which defeats the performance
            # benefits of view.
            clsname = self.__class__.__name__
            warnings.warn("The requested query cannot produce perfect SQL "
                          "with a %s datasource. It may take an absurdly "
                          "long time to run, since each unit must be fully-"
                          "formed. %s" % (clsname, query), StorageWarning)
            return storage.StorageManager.count(self, cls, expr)
        else:
            return data.scalar()
    
    def _xmultirecall(self, classes, expr=None, order=None, limit=None, offset=None):
        """Yield Unit instance sets which satisfy the expression."""
        if self.logflags & logflags.RECALL:
            self.log(logflags.RECALL.message(classes, expr))
        
        # Gather attribute list.
        allattrs = []
        props = []
        for cls in classes:
            attrs = []
            for key in cls.properties:
                attrs.append(key)
                props.append((cls, key, getattr(cls, key).coerce))
            allattrs.append(attrs)
        
        data = self.select((classes, allattrs, expr), order=order,
                           limit=limit, offset=offset)
        for row in data:
            # TODO: This is broken; won't work if same cls appears twice.
            units = {}
            for i, (cls, key, propcoerce) in enumerate(props):
                if cls in units:
                    unit = units[cls]
                else:
                    unit = cls.__new__(cls)
                    unit._zombie = True
                    unit.__init__()
                    units[cls] = unit
                
                value = row[i]
                try:
                    if propcoerce:
                        value = propcoerce(unit, value)
                    unit._properties[key] = value
                except Exception, x:
                    x.args += (cls, key)
                    raise
            
            unitset = []
            for cls in classes:
                unit = units[cls]
                unit.cleanse()
                unitset.append(unit)
            
            # If our SQL is imperfect, don't yield units to the
            # caller unless they pass expr(unit).
            acceptable = True
            if expr and data.statement.imperfect:
                acceptable = expr(*unitset)
            if acceptable:
                yield unitset
    
    #                               Schemas                               #
    
    def create_database(self, conflicts='error'):
        """Create internal structures for the entire database.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("create database"))
        
        try:
            self.db.create()
            self.schema.create()
        except geniusql.errors.MappingError, x:
            conflict(conflicts, str(x))
    
    def has_database(self):
        """If storage exists for this database, return True."""
        return self.db.exists()
    
    def drop_database(self, conflicts='error'):
        """Destroy internal structures for the entire database.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop database"))
        
        try:
            self.schema.drop()
        except geniusql.errors.MappingError, x:
            conflict(conflicts, str(x))
        
        try:
            self.db.drop()
        except geniusql.errors.MappingError, x:
            conflict(conflicts, str(x))
    
    def _make_table(self, cls):
        """Create and return a Table or View object for the given class."""
        if hasattr(cls, "view_statement"):
            gs = self._geniusql_statement(cls.view_statement)
            t = self.schema.view(cls.__name__, gs)
            fields = []
            for key in cls.properties:
                t[key] = self._make_column(cls, key)
        else:
            t = self.schema.table(cls.__name__)
            indices = cls.indices()
            fields = []
            for key in cls.properties:
                t[key] = col = self._make_column(cls, key)
                if key in indices:
                    t.add_index(key)
                if col.autoincrement and col.sequence_name is None:
                    # Not every database needs/uses sequence_name
                    col.sequence_name = self.schema.sequence_name(t.name, key)
        
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
            # Attach to self.schema, which should call CREATE TABLE.
            self.schema[cls.__name__] = self._make_table(cls)
        except geniusql.errors.MappingError, x:
            conflict(conflicts, str(x))
    
    def _make_column(self, cls, key):
        prop = getattr(cls, key)
        col = self.schema.column(prop.type, prop.hints.get('dbtype'),
                                 default=prop.default, hints=prop.hints)
        if key in cls.identifiers:
            col.key = True
            if isinstance(cls.sequencer, dejavu.UnitSequencerInteger):
                col.autoincrement = True
                col.initial = cls.sequencer.initial
        return col
    
    def has_storage(self, cls):
        """If storage structures exist for the given class, return True."""
        return cls.__name__ in self.schema
    
    def drop_storage(self, cls, conflicts='error'):
        """Destroy internal structures for the given class.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop storage %s" % cls))
        
        try:
            del self.schema[cls.__name__]
        except geniusql.errors.MappingError, x:
            conflict(conflicts, str(x))
    
    def rename_storage(self, oldname, newname, conflicts='error'):
        """Rename internal structures for the given class.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("rename storage from %s to %s"
                                          % (oldname, newname)))
        
        try:
            self.schema.rename(oldname, newname)
        except geniusql.errors.MappingError, x:
            conflict(conflicts, str(x))
    
    def add_property(self, cls, name, conflicts='error'):
        """Add internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("add property %s %s" %
                                          (cls, name)))
        
        if not self.has_property(cls, name):
            try:
                table = self.schema[cls.__name__]
                table[name] = col = self._make_column(cls, name)
                if col.autoincrement and col.sequence_name is None:
                    # Not every database needs/uses sequence_name
                    col.sequence_name = self.schema.sequence_name(table.name, name)
            except geniusql.errors.MappingError, x:
                conflict(conflicts, str(x))
    
    def has_property(self, cls, name):
        """If storage structures exist for the given property, return True."""
        return name in self.schema[cls.__name__]
    
    def drop_property(self, cls, name, conflicts='error'):
        """Destroy internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop property %s %s" %
                                          (cls, name)))
        if self.has_property(cls, name):
            try:
                del self.schema[cls.__name__][name]
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
            t = self.schema[cls.__name__]
            
            # Sometimes, a Dejavu Schema will change a code model first, and
            # then change the database afterward. So it's possible that the
            # column we're trying to rename hasn't been loaded, because the
            # model layer no longer references it. So if table[oldname]
            # raises a KeyError, try to find a column that matches oldkey.
            tempcol = None
            try:
                t[oldname]
            except KeyError:
                c = [x for x in self.schema._get_columns(t.name)
                     if x.name == self.schema.column_name(t.name, oldname)]
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
        """Add an index for the given property.
        
        conflicts: see errors.conflict.
        """
        try:
            self.schema[cls.__name__].add_index(name)
        except geniusql.errors.MappingError, x:
            conflict(conflicts, str(x))
    
    def has_index(self, cls, name):
        """If an index exists for the given property, return True."""
        return name in self.schema[cls.__name__].indices
    
    def drop_index(self, cls, name, conflicts='error'):
        """Destroy any index on the given property.
        
        conflicts: see errors.conflict.
        """
        try:
            del self.schema[cls.__name__].indices[name]
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
            self.db.discover_dbinfo()
            self.sync(classes, conflicts)
        else:
            for cls in classes:
                if self.has_storage(cls):
                    # If our consumer-side key is already present, skip this cls.
                    # This allows callers to auto-sync class by class
                    # without making a new Table object each time.
                    continue
                
                t = self._make_table(cls)
                
                # Use the superclass call to avoid DROP/CREATE TABLE
                dict.__setitem__(self.schema, cls.__name__, t)
    
    def sync(self, classes, conflicts='error'):
        """Map classes to existing Table objects (found via discovery).
        
        conflicts: see errors.conflict.
        """
        for cls in classes:
            if cls.__name__ in self.schema:
                # If our consumer-side key is already present, skip this cls.
                # This allows callers to auto-sync class by class
                # without calling the expensive discover() func each time.
                continue
            self._find_table(self.schema, cls, conflicts=conflicts)
    
    def _find_table(self, schema, cls, conflicts='error'):
        # This is broken out to make multi-schema subclasses easier to write.
        
        # Try to find a matching Table or View object using the DB-side key.
        clsname = cls.__name__
        tablename = schema.table_name(clsname)
        try:
            # Do we already have a map using the DB name?
            table = schema[tablename]
            schema.alias(table.name, clsname)
        except KeyError:
            # Can we create a map? Discover the DB table and try again.
            try:
                table = schema.discover(tablename)
                schema.alias(table.name, clsname)
            except geniusql.errors.MappingError:
                msg = "%s: no such table %r." % (clsname, tablename)
                if self.logflags & logflags.DDL:
                    self.log(logflags.DDL.message(msg))
                if conflicts == 'repair':
                    # Didn't have one. Couldn't find one. Let's make one.
                    self.create_storage(cls)
                    table = schema[clsname]
                else:
                    conflict(conflicts, msg)
                    return
        
        # Match Column objects with class properties.
        dbcols = dict([(c.name, c) for c in table.itervalues()])
        indices = cls.indices()
        for pkey in cls.properties:
            colname = schema.column_name(table.name, pkey)
            try:
                col = dbcols[colname]
                table.alias(colname, pkey)
            except KeyError, x:
                msg = "%s: no column found for %r." % (clsname, pkey)
                if self.logflags & logflags.DDL:
                    self.log(logflags.DDL.message(msg))
                if conflicts == 'repair':
                    self.add_property(cls, pkey)
                    if pkey in cls.indices() and pkey not in table.indices:
                        self.add_index(cls, pkey)
                    col = table[pkey]
                else:
                    conflict(conflicts, msg)
                    continue
            
            # Check that the column.key matches our identifiers list;
            # this is crucial for the proper operation of OLTP methods
            # in geniusql.Table, which uses column.key to decide
            # the unique identifiers for a given row of data.
            if not isinstance(table, geniusql.View):
                if pkey in cls.identifiers and not col.key:
                    msg = ("%s: %r is an identifier, but the "
                           "column is not marked as a primary key."
                           % (clsname, pkey))
                    if self.logflags & logflags.DDL:
                        self.log(logflags.DDL.message(msg))
                    if conflicts == 'repair':
                        col.key = True
                        table.set_primary()
                    else:
                        conflict(conflicts, msg)
                        continue
                elif col.key and not pkey in cls.identifiers:
                    msg = ("%s: %r is not an identifier, but the "
                           "column is marked as a primary key."
                           % (clsname, pkey))
                    if self.logflags & logflags.DDL:
                        self.log(logflags.DDL.message(msg))
                    if conflicts == 'repair':
                        col.key = False
                        # Just because the current pkey is not an identifier
                        # doesn't mean we have *no* identifiers.
                        table.set_primary()
                    else:
                        conflict(conflicts, msg)
                        continue
            
            col.pytype = getattr(cls, pkey).type
            
            # Override the default adapter (since it guessed an adapter
            # using the default pytype, and we know better).
            try:
                col.adapter = col.dbtype.default_adapter(col.pytype)
            except TypeError, x:
                x.args += ("%s.%s" % (table.name, col.name),)
                raise
            
            if not hasattr(table, "indices"):
                # Skip indices for Views.
                continue
            
            # Try to find matching Index objects. Because index names are
            # so platform-specific, we match attributes rather than names.
            if pkey in indices:
                for ikey, idx in table.indices.items():
                    if idx.colname == colname:
                        table.indices.alias(ikey, schema.index_name(table, pkey))
                        break
                else:
                    msg = "%s: no index found for %r." % (clsname, pkey)
                    if self.logflags & logflags.DDL:
                        self.log(logflags.DDL.message(msg))
                    if conflicts == 'repair':
                        self.add_index(cls, pkey)
                    else:
                        conflict(conflicts, msg)
                        continue
            else:
                if pkey in cls.identifiers and self.db.pks_must_be_indexed:
                    pass
                else:
                    for ikey, idx in table.indices.items():
                        if idx.colname == colname:
                            msg = ("%s: index found for non-indexed %r."
                                   % (clsname, pkey))
                            if self.logflags & logflags.DDL:
                                self.log(logflags.DDL.message(msg))
                            if conflicts == 'repair':
                                self.drop_index(cls, ikey)
                            else:
                                conflict(conflicts, msg)
                                continue
        
        # Set Table.references
        for k, v in cls._associations.iteritems():
            table.references[k] = (v.nearKey, v.farClass.__name__, v.farKey)
        
        return table
    
    #                            Transactions                             #
    
    def start(self, isolation=None):
        "Start a transaction (not needed if db.connections.implicit_trans)."
        self.db.connections.start(isolation)
    
    def rollback(self):
        """Roll back the current transaction."""
        self.db.connections.rollback()
    
    def commit(self):
        """Commit the current transaction."""
        self.db.connections.commit()


class Modeler(object):
    """Tool to automatically form Unit classes or source from existing DB's."""
    
    ignore = ['Unit', 'DeployedVersion',
              'UnitEngine', 'UnitEngineRule', 'UnitCollection',
              ]
    
    def __init__(self, schema):
        self.schema = schema
        self.ignore = self.ignore[:]
    
    def all_classes(self):
        """Return a list of new classes for all tables in the Database."""
        ignore = dict.fromkeys([self.schema.table_name(x) for x in self.ignore]
                               + self.ignore).keys()
        
        self.schema.discover_all(ignore=ignore)
        
        classes = []
        seen = {}
        for key, table in self.schema.items():
            if key not in ignore and table.name not in seen:
                cls = self.make_class(key)
                classes.append(cls)
                seen[table.name] = None
        return classes
    
    def make_class(self, tablename, newclassname=None):
        """Create a Unit class automatically from the named table."""
        if tablename not in self.schema:
            self.schema.discover(tablename)
        table = self.schema[tablename]
        
        class AutoUnitClass(dejavu.Unit):
            sequencer = dejavu.UnitSequencer()
            identifiers = tuple([k for k in table if table[k].key])
        
        if newclassname is None:
            newclassname = table.name
            # The key is probably better than the table.name. Try it.
            for key, t in self.schema.iteritems():
                if t.name == newclassname:
                    newclassname = key
                    break
        AutoUnitClass.__name__ = newclassname
        
        indices = [idx.colname for idx in table.indices.itervalues()]
        for cname, c in table.iteritems():
            ptype = c.pytype
            if ptype == int and c.dbtype.bytes == 1:
                # This is probably a bool
                ptype = bool
            p = AutoUnitClass.set_property(cname, ptype)
            if c.autoincrement:
                AutoUnitClass.sequencer = dejavu.UnitSequencerInteger(int, c.initial)
            p.default = c.default
            
            p.hints = dict([(k, getattr(c.dbtype, k))
                            for k in ("bytes", "precision", "scale")
                            if hasattr(c.dbtype, k)])
            if p.hints:
                # Postgresql hack: replace bytes=ComparableInfinity with 0,
                # since 0 signifies "no limit".
                for k, v in p.hints.iteritems():
                    if v.__class__.__name__ == 'ComparableInfinity':
                        p.hints[k] = 0
            
            p.index = (cname in indices)
        
        # Remove default ID property if necessary.
        if "ID" not in table:
            AutoUnitClass.properties.remove('ID')
            AutoUnitClass.ID = None
        
        return AutoUnitClass
    
    def all_source(self):
        """Return a list of strings of Unit source code for all tables."""
        ignore = dict.fromkeys([self.schema.table_name(x) for x in self.ignore]
                               + self.ignore).keys()
        
        self.schema.discover_all(ignore=ignore)
        
        allcode = []
        seen = {}
        tables = self.schema.items()
        tables.sort()
        for key, table in tables:
            if key not in ignore and table.name not in seen:
                code = self.make_source(key)
                allcode.append(code)
                seen[table.name] = None
        return allcode
    
    def make_source(self, tablename, newclassname=None):
        """Create source code for a Unit class from the named table."""
        if tablename not in self.schema:
            self.schema.discover(tablename)
        table = self.schema[tablename]
        
        code = []
        
        if newclassname is None:
            newclassname = table.name
            # The key is probably better than the table.name. Try it.
            for key, t in self.schema.iteritems():
                if t.name == newclassname:
                    newclassname = key
                    break
            # Make the name safe for use as a Python class name
            newclassname = newclassname.replace(".", "_")
        code.append("class %s(Unit):" % newclassname)
        
        if table.description:
            import textwrap
            block = textwrap.fill(table.description, subsequent_indent='    ')
            code.append('    """%s"""' % block)
        
        sequencer = None
        indices = [idx.colname for idx in table.indices.itervalues()]
        
        # iterate over all columns
        columns = table.items()
        columns.sort()
        for cname, c in columns:
            prop, seq = self._make_column_source(cname, c, cname in indices)
            if not prop.startswith("    ID = UnitProperty(int"):
                code.append(prop)
            if seq:
                sequencer = seq
        
        # Remove default ID property if necessary.
        if "ID" not in table:
            code.append("    # Remove the default 'ID' property.")
            code.append("    ID = None")
        
        pk = tuple([k for k in table if table[k].key])
        if pk != ("ID",):
            code.append("    identifiers = %s" % repr(pk))
        
        if sequencer:
            if sequencer != "    sequencer = UnitSequencerInteger(int, 1)":
                code.append(sequencer)
        else:
            code.append("    sequencer = UnitSequencer()")
        
        if len(code) == 1:
            code.append("    pass")
        
        return "\n".join(code)
    
    def _make_column_source(self, colname, column, has_index):
        ptype = column.pytype
        if ptype == int and column.dbtype.bytes == 1:
            # This is probably a bool
            ptype = bool
        
        mod = ptype.__module__
        if mod == '__builtin__':
            ptype = ptype.__name__
        else:
            ptype = mod + "." + ptype.__name__
        
        seq = None
        if column.autoincrement:
            seq = ("    sequencer = UnitSequencerInteger(int, %r)" %
                   column.initial)
        
        default = column.default
        if default is None:
            default = ""
        else:
            default = ", default=%r" % default
        
        index = ""
        if has_index:
            index = ", index=True"
        
        hints = dict([(k, getattr(column.dbtype, k))
                      for k in ("bytes", "precision", "scale")
                      if hasattr(column.dbtype, k)])
        if hints:
            # Postgresql hack: replace bytes=ComparableInfinity with 0,
            # since 0 signifies "no limit".
            for k, v in hints.iteritems():
                if v.__class__.__name__ == 'ComparableInfinity':
                    hints[k] = 0
            hints = ", hints=%r" % hints
        else:
            hints = ""
        
        return ("    %s = UnitProperty(%s%s%s%s)" %
                (colname, ptype, index, hints, default)), seq

