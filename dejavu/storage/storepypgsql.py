from geniusql.providers import pypgsql
from dejavu.storage import db, multischema


class StorageManagerPgSQL(db.StorageManagerDB):
    """StoreManager to save and retrieve Units via pyPgSQL 1.35."""
    
    databaseclass = pypgsql.PyPgDatabase
    
    def __init__(self, allOptions={}):
        for atom in allOptions['connections.Connect'].split(" "):
            k, v = atom.split("=", 1)
            if k == "dbname":
                allOptions['name'] = v
        db.StorageManagerDB.__init__(self, allOptions)


class MultiSchemaStorageManagerPg(multischema.MultiSchemaStorageManagerDB):
    """StoreManager to save and retrieve multiple schemas via pyPgSQL."""
    
    databaseclass = pypgsql.PyPgDatabase
    
    def __init__(self, allOptions={}):
        for atom in allOptions['connections.Connect'].split(" "):
            k, v = atom.split("=", 1)
            if k == "dbname":
                allOptions['name'] = v
        multischema.MultiSchemaStorageManagerDB.__init__(self, allOptions)
