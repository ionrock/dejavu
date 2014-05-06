try:
    import cPickle as pickle
except ImportError:
    import pickle

import os
import shutil
import threading
import time

import dejavu
from dejavu import errors, json, logflags, storage

from geniusql import logic


class StorageManagerJSON(storage.StorageManager):
    """StoreManager to save and retrieve Units in flat files as JSON.
    
    This is slightly different from fs, in that this saves each Unit class
    to its own folder, and each Unit instance to its own file.
    
    Be aware that this SM stores each Unit in its own file,
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
        # into a single file name.
        self.idsepchar = allOptions.get('idsepchar', '_')
        
        encoding = allOptions.get('encoding', None)
        self.decoder = json.Decoder(encoding=encoding)
        
        skipkeys = allOptions.get('skipkeys', False)
        check_circular = allOptions.get('check_circular', True)
        allow_nan = allOptions.get('allow_nan', False)
        indent = allOptions.get('indent', None)
        self.encoder = json.Encoder(skipkeys, ensure_ascii=True,
                                    check_circular=True, allow_nan=True,
                                    indent=indent)
    
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
    
    def _push(self, root, fname, data):
        """Persist a unit property dict into its file.
        
        This assumes the folder exists and we have a lock on it.
        """
        fname = os.path.join(root, fname)
        f = open(fname, 'wb')
        try:
            f.write(self.encoder.encode(data))
        finally:
            f.close()
    
    def _pull(self, root, fname):
        """Return a unit property dict from the given filename, or None."""
        fname = os.path.join(root, fname)
        v = open(fname, 'rb').read()
        return self.decoder.decode(v)
    
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
            fname = self.idsepchar.join([str(kwargs[k])
                                         for k in cls.identifiers])
            if not fname:
                fname = "__blank__"
            
            try:
                data = self._pull(classdir, fname)
                unit = json.dict_to_unit(data, cls=cls)
            except IOError:
                return None
            
            unit.cleanse()
            return unit
        
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
            files = os.listdir(path)
        finally:
            self.release_lock(cls)
        
        data = self._xrecall_inner(cls, expr, path, files)
        return self._paginate(data, order, limit, offset, single=True)
    
    def _xrecall_inner(self, cls, expr, root, files):
        """Private helper for self.xrecall."""
        for fname in files:
            if fname == "class.lock":
                continue
            
            data = self._pull(root, fname)
            unit = json.dict_to_unit(data, cls=cls)
            if expr is None or expr(unit):
                unit.cleanse()
                # Must yield a sequence for use in _paginate.
                yield (unit,)
    
    def ids_from_file(self, cls, fname):
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
    
    def filename_from_unit(self, unit):
        """Return the folder name for the given unit."""
        if unit.identifiers:
            folder = self.idsepchar.join([str(getattr(unit, k))
                                          for k in unit.identifiers])
        else:
            folder = str(hash(unit))
        
        if not folder:
            folder = "__blank__"
        return folder
    
    def reserve(self, unit):
        """Reserve a persistent slot for unit."""
        if self.logflags & logflags.RESERVE:
            self.log(logflags.RESERVE.message(unit))
        
        cls = unit.__class__
        path = self.shelf(cls)
        self.get_lock(cls)
        try:
            if not unit.sequencer.valid_id(unit.identity()):
                files = os.listdir(path)
                ids = [[v for k, v in self.ids_from_file(cls, fname)]
                       for fname in files if fname != 'class.lock']
                unit.sequencer.assign(unit, ids)
            
            fname = self.filename_from_unit(unit)
            self._push(path, fname, json.unit_to_dict(unit))
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
                fname = self.filename_from_unit(unit)
                self._push(path, fname, json.unit_to_dict(unit))
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
            fname = self.filename_from_unit(unit)
            os.unlink(os.path.join(path, fname))
        finally:
            self.release_lock(cls)
    
    __version__ = "0.1"
    
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
        
        path = self.shelf(cls)
        self.get_lock(cls)
        try:
            for fname in os.listdir(path):
                if fname == "class.lock":
                    continue
                try:
                    data = self._pull(path, fname)
                    data[name] = None
                    self._push(path, fname, data)
                except Exception, x:
                    errors.conflict(conflicts, str(x))
        finally:
            self.release_lock(cls)
    
    def has_property(self, cls, name):
        """If storage structures exist for the given property, return True."""
        path = self.shelf(cls)
        self.get_lock(cls)
        try:
            for fname in os.listdir(path):
                if fname == "class.lock":
                    continue
                return name in self._pull(path, fname)
        finally:
            self.release_lock(cls)
    
    def drop_property(self, cls, name, conflicts='error'):
        """Destroy internal structures for the given property.
        
        conflicts: see errors.conflict.
        """
        if self.logflags & logflags.DDL:
            self.log(logflags.DDL.message("drop property %s %s"
                                          % (cls, name)))
        
        path = self.shelf(cls)
        self.get_lock(cls)
        try:
            for fname in os.listdir(path):
                if fname == "class.lock":
                    continue
                try:
                    data = self._pull(path, fname)
                    data.pop(name, None)
                    self._push(path, fname, data)
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
        
        path = self.shelf(cls)
        self.get_lock(cls)
        try:
            for fname in os.listdir(path):
                if fname == "class.lock":
                    continue
                try:
                    data = self._pull(path, fname)
                    data[newname] = data.pop(oldname, None)
                    self._push(path, fname, data)
                except Exception, x:
                    errors.conflict(conflicts, str(x))
        finally:
            self.release_lock(cls)
