
from geniusql.providers import psycopg
from dejavu.storage import db, multischema


class StorageManagerPsycoPg(db.StorageManagerDB):
    """StoreManager to save and retrieve Units via psycopg2."""
    
    databaseclass = psycopg.PsycoPgDatabase
    
    def __init__(self, allOptions={}):
        for atom in allOptions['connections.Connect'].split(" "):
            k, v = atom.split("=", 1)
            if k == "dbname":
                allOptions['name'] = v
        db.StorageManagerDB.__init__(self, allOptions)


class MultiSchemaStorageManagerPg(multischema.MultiSchemaStorageManagerDB):
    """StoreManager to save and retrieve multiple schemas via psycopg2."""
    
    databaseclass = psycopg.PsycoPgDatabase
    
    def __init__(self, allOptions={}):
        for atom in allOptions['connections.Connect'].split(" "):
            k, v = atom.split("=", 1)
            if k == "dbname":
                allOptions['name'] = v
        multischema.MultiSchemaStorageManagerDB.__init__(self, allOptions)

