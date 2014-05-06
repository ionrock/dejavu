import warnings

from geniusql.providers import ado, sqlserver, msaccess
from dejavu import errors
from dejavu.storage import db


class StorageManagerADO_SQLServer(db.StorageManagerDB):
    """StoreManager to save and retrieve Units via ADO 2.7.
    
    You must run makepy on ADO 2.7 before installing.
    """
    
    databaseclass = sqlserver.SQLServerDatabase
    
    def __init__(self, allOptions={}):
        atoms = ado.connatoms(allOptions['connections.Connect'])
        allOptions['name'] = atoms.get('INITIAL CATALOG') or atoms.get('DSN')
        db.StorageManagerDB.__init__(self, allOptions)


class StorageManagerADO_MSAccess(db.StorageManagerDB):
    """StoreManager to save and retrieve Units via ADO 2.7.
    
    You must run makepy on ADO 2.7 before installing.
    """
    # Jet Connections and Recordsets are always free-threaded.
    
    databaseclass = msaccess.MSAccessDatabase
    
    def __init__(self, allOptions={}):
        atoms = ado.connatoms(allOptions['connections.Connect'])
        allOptions['name'] = (atoms.get('DATA SOURCE') or
                              atoms.get('DATA SOURCE NAME') or
                              atoms.get('DBQ'))
        db.StorageManagerDB.__init__(self, allOptions)
    
    def _make_column(self, cls, key):
        col = db.StorageManagerDB._make_column(self, cls, key)
        if col.dbtype == "MEMO":
            for assoc in cls._associations.itervalues():
                if assoc.nearKey == key:
                    warnings.warn("Memo fields cannot be used as join keys. "
                                  "You should set %s.%s(hints={'bytes': 255})"
                                  % (cls.__name__, key), errors.StorageWarning)
        return col

