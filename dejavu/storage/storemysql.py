from geniusql.providers import mysql
from dejavu.storage import db


class StorageManagerMySQL(db.StorageManagerDB):
    """StoreManager to save and retrieve Units via _mysql."""
    
    databaseclass = mysql.MySQLDatabase
    
    def __init__(self, allOptions={}):
        allOptions['name'] = allOptions['db']
        db.StorageManagerDB.__init__(self, allOptions)

