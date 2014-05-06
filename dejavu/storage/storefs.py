try:
    import cPickle as pickle
except ImportError:
    import pickle

import os
import shutil
import threading
import time

import dejavu
from dejavu import errors, logflags, storage

from geniusql import logic


class StorageManagerFolders(storage.StorageManager):
    """StoreManager to save and retrieve Units in flat files.
    
    This is slightly different from shelve, in that this saves
    each Unit to its own folder, and each property to its own file.
    This is useful for storing large items like images and video
    for which you want both a native, readable file for the data
    and also the ability to store metadata.
    
    Unit properties which you want to use for binary data should
    be of type 'str'. The filename will be the property name,
    and the extension should be supplied in config.
    
    Be aware that this SM stores each Unit in its own folder,
    and your operating system may have hard limits or performance
    issues when the number of Units of a given class (or the number
    of classes) grows large.
    """
    
    def __init__(self, allOptions={}):
        storage.StorageManager.__init__(self, allOptions)
        
        root = allOptions['root']
        if not os.path.isabs(root):
            root = os.path.join(os.getcwd(), root)
        self.root = root
        
        self.mode = int(allOptions.get('mode', '0777'), 8)
        
        # Character to use for joining multiple identifiers
        # into a single folder name.
        self.idsepchar = allOptions.get('idsepchar', '_')
        
        # Map of file extensions. Keys should be "clsname.propname"
        # and values should contain the dot (if desired).
        self.extmap = dict([(k, v) for k, v in allOptions.iteritems()
                            if '.' in k])
        self.extdefault = allOptions.get('extdefault', '.txt')
    
    def shelf(self, cls):
        """Return path for the given cls."""
        return os.path.join(self.root, cls.__name__)
    
    def get_lock(self, cls):
        clsname = cls.__name__
        path = os.path.join(self.root, clsname, "class.lock")
        while True:
            try:
                lockfd = os.open(path, os.O_CREAT|os.O_WRONLY|os.O_EXCL)
            except OSError:
                time.sleep(0.1)
            else:
                os.close(lockfd) 
                break
    
    def release_lock(self, cls):
        clsname = cls.__name__
        path = os.path.join(self.root, clsname, "class.lock")
        os.unlink(path)
    
    def _pull(self, cls, root, idset):
        """Return data from the given path as a dict."""
        
        # Grab identifiers from the folder name.
        unitdict = dict(self.ids_from_folder(cls, idset))
        
        # Grab values from the files in the folder.
        clsname = cls.__name__
        for k in cls.properties:
            extkey = "%s.%s" % (clsname, k)
            ext = self.extmap.get(extkey, self.extdefault)
            
            fname = os.path.join(root, idset, "%s%s" % (k, ext))
            try:
                v = open(fname, 'rb').read()
                if getattr(cls, k).type is not str:
                    v = pickle.loads(v)
            except (EOFError, IOError), exc:
                v = None
            unitdict[k] = v
        
        return unitdict
    
    def unit(self, cls, **kwargs):
        """A single Unit which matches the given kwargs, else None.
        
        The first Unit matching the kwargs is returned; if no Units match,
        None is returned.
        """
        if set(kwargs.keys()) == set(cls.identifiers):
            # Looking up a Unit by its identifiers.
            # Skip walking the shelf.
            if self.logflags & logflags.RECALL:
                self.log(logflags.RECALL.message(cls, kwargs))
            
            classdir = self.shelf(cls)
            folder = self.idsepchar.join([str(kwargs[k])
                                          for k in cls.identifiers])
            if not folder:
                folder = "__blank__"
            
            if os.path.exists(os.path.join(classdir, folder)):
                unit = cls()
                unit._properties = self._pull(cls, classdir, folder)
                unit.cleanse()
                return unit
            else:
                return None
        
        return storage.StorageManager.unit(self, cls, **kwargs)
    
    def xrecall(self, classes, expr=None, order=None, limit=None, offset=None):
        if isinstance(classes, dejavu.UnitJoin):
            return self._xmultirecall(classes, expr, order=order,
                                      limit=limit, offset=offset)
        
        cls = classes
        if self.logflags & logflags.RECALL:
            self.log(logflags.RECALL.message(cls, expr))
        
        path = self.shelf(cls)
        self.get_lock(cls)
        try:
            root, dirs, _ = os.walk(path).next()
        finally:
            self.release_lock(cls)
        
        if not isinstance(expr, logic.Expression):
            expr = logic.Expression(expr)
        
        data = self._xrecall_inner(cls, expr, root, dirs)
        return self._paginate(data, order, limit, offset, single=True)
    
    def _xrecall_inner(self, cls, expr, root, dirs):
        """Private helper for self.xrecall."""
        for idset in dirs:
            unit = cls()
            unit._properties = self._pull(cls, root, idset)
            if expr is None or expr(unit):
                unit.cleanse()
                # Must yield a sequence for use in _paginate.
                yield (unit,)
    
    def ids_from_folder(self, cls, fname):
        """Return a list of identifier (k, v) pairs for the named folder."""
        if cls.identifiers:
            if fname == "__blank__":
                return [(k, "") for k in cls.identifiers]
            
            # cls.identifiers is ordered, and should match
            # the order of atoms inside fname.
            return [(k, getattr(cls, k).coerce(None, v))
                    for k, v in zip(cls.identifiers,
                                    fname.split(self.idsepchar))
                    ]
        else:
            return []
    
    def folder_from_unit(self, unit):
        """Return the folder name for the given unit."""
        if unit.identifiers:
            folder = self.idsepchar.join([str(getattr(unit, k))
                                          for k in unit.identifiers])
        else:
            folder = str(hash(unit))
        
        if not folder:
            folder = "__blank__"
        return folder
    
    def _push(self, unit, abspath):
        """Persist unit properties into its folder.
        
        This assumes the folder exists and we have a lock on it.
        """
        cls = unit.__class__
        for key, value in unit._properties.iteritems():
            extkey = "%s.%s" % (cls.__name__, key)
            ext = self.extmap.get(extkey, self.extdefault)
            fname = "%s%s" % (key, ext)
            fname = os.path.join(abspath, fname)
            try:
                f = open(fname, 'wb')
            except IOError:
                raise
            
            try:
                if getattr(cls, key).type is not str:
                    value = pickle.dumps(value)
                f.write(value)
            finally:
                f.close()
    
    def reserve(self, unit):
        """Reserve a persistent slot for unit."""
        if self.logflags & logflags.RESERVE:
            self.log(logflags.RESERVE.message(unit))
        
        cls = unit.__class__
        path = self.shelf(cls)
        self.get_lock(cls)
        try:
            if not unit.sequencer.valid_id(unit.identity()):
                root, dirs, _ = os.walk(path).next()
                ids = [[v for k, v in self.ids_from_folder(cls, dirname)]
                       for dirname in dirs]
                unit.sequencer.assign(unit, ids)
            
            fname = self.folder_from_unit(unit)
            fname = os.path.join(path, fname)
            if not os.path.exists(fname):
                os.mkdir(fname, self.mode)
            self._push(unit, fname)
        finally:
            self.release_lock(cls)
            unit.cleanse()
    
    def save(self, unit, forceSave=False):
        """Update storage from unit's data."""
        if self.logflags & logflags.SAVE:
            self.log(logflags.SAVE.message(unit, forceSave))
        
        if forceSave or unit.dirty():
            cls = unit.__class__
            path = self.shelf(cls)
            self.get_lock(cls)
            try:
                fname = self.folder_from_unit(unit)
                self._push(unit, os.path.join(path, fname))
            finally:
                self.release_lock(cls)
                unit.cleanse()
    
    def destroy(self, unit):
        """Delete the unit."""
        if self.logflags & logflags.DESTROY:
            self.log(logflags.DESTROY.message(unit))
        
        cls = unit.__class__
        path = self.shelf(cls)
        self.get_lock(cls)
        try:
            fname = self.folder_from_unit(unit)
            shutil.rmtree(os.path.join(path, fname))
        finally:
            self.release_lock(cls)
    
    __version__ = "0.2"
    
    def version(self):
        return "%s %s" % (self.__class__.__name__, self.__version__)
    
    
    #                               Schemas                               #
    
    def create_database(self, conflicts='error'):
        """Create internal structures for the entire database.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("create database"))
        
        try:
            os.makedirs(self.root)
        except Exception, x:
            errors.conflict(conflicts, str(x))
    
    def has_database(self):
        """If storage exists for this database, return True."""
        return os.path.exists(self.root)
    
    def drop_database(self, conflicts='error'):
        """Destroy internal structures for the entire database.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop database"))
        
        try:
            shutil.rmtree(self.root)
        except Exception, x:
            errors.conflict(conflicts, str(x))
    
    def create_storage(self, cls, conflicts='error'):
        """Destroy internal structures for the given class.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("create storage %s" % cls))
        
        try:
            path = self.shelf(cls)
            os.mkdir(path, self.mode)
        except Exception, x:
            errors.conflict(conflicts, str(x))
    
    def has_storage(self, cls):
        """If storage structures exist for the given class, return True."""
        return os.path.exists(self.shelf(cls))
    
    def drop_storage(self, cls, conflicts='error'):
        """Destroy internal structures for the given class.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop storage %s" % cls))
        
        try:
            shutil.rmtree(self.shelf(cls))
        except Exception, x:
            errors.conflict(conflicts, str(x))
    
    def add_property(self, cls, name, conflicts='error'):
        """Add internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("add property %s %s"
                                          % (cls, name)))
        pass
    
    def has_property(self, cls, name):
        """If storage structures exist for the given property, return True."""
        return True
    
    def drop_property(self, cls, name, conflicts='error'):
        """Destroy internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop property %s %s"
                                          % (cls, name)))
        
        extkey = "%s.%s" % (cls.__name__, name)
        ext = self.extmap.get(extkey, self.extdefault)
        fname = "%s%s" % (name, ext)
        
        path = self.shelf(cls)
        self.get_lock(cls)
        try:
            root, dirs, _ = os.walk(path).next()
            for idset in dirs:
                try:
                    os.remove(os.path.join(root, idset, fname))
                except Exception, x:
                    errors.conflict(conflicts, str(x))
        finally:
            self.release_lock(cls)
    
    def rename_property(self, cls, oldname, newname, conflicts='error'):
        """Rename internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message(
                "rename property %s from %s to %s"
                % (cls, oldname, newname)))
        
        extkey = "%s.%s" % (cls.__name__, oldname)
        ext = self.extmap.get(extkey, self.extdefault)
        oname = "%s%s" % (oldname, ext)
        
        extkey = "%s.%s" % (cls.__name__, newname)
        ext = self.extmap.get(extkey, self.extdefault)
        nname = "%s%s" % (newname, ext)
        
        path = self.shelf(cls)
        self.get_lock(cls)
        try:
            root, dirs, _ = os.walk(path).next()
            for idset in dirs:
                try:
                    os.rename(os.path.join(root, idset, oname),
                              os.path.join(root, idset, nname))
                except Exception, x:
                    errors.conflict(conflicts, str(x))
        finally:
            self.release_lock(cls)

