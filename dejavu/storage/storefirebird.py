from geniusql.providers import firebird
from dejavu.storage import db


class StorageManagerFirebird(db.StorageManagerDB):
    """StoreManager to save and retrieve Units via Firebird 1.5."""
    
    databaseclass = firebird.FirebirdDatabase
