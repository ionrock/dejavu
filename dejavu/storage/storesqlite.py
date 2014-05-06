import os

from geniusql.providers import sqlite
from dejavu.storage import db


class StorageManagerSQLite(db.StorageManagerDB):
    """StoreManager to save and retrieve Units via _sqlite."""
    
    databaseclass = sqlite.SQLiteDatabase
    
    def __init__(self, allOptions={}):
        allOptions = allOptions.copy()
        allOptions['name'] = allOptions.pop('Database', '')
        
        pd = str(allOptions.get('Perfect Dates', 'False')).lower()
        allOptions['using_perfect_dates'] = (pd == "true")
        
        db.StorageManagerDB.__init__(self, allOptions)
    
    def reserve(self, unit):
        """Reserve a persistent slot for unit."""
        self.reserve_lock.acquire()
        try:
            # First, see if our db subclass has a handler that
            # uses the DB to generate the appropriate identifier(s).
            seqclass = unit.sequencer.__class__.__name__
            seq_handler = getattr(self, "_seq_%s" % seqclass, None)
            if (seq_handler and
                   (seqclass != "UnitSequencerInteger" or
                    sqlite._autoincrement_support)):
                seq_handler(unit)
            else:
                self._manual_reserve(unit)
            unit.cleanse()
        finally:
            self.reserve_lock.release()
    
    #                               Schemas                               #
    
    def _make_column(self, cls, key):
        col = db.StorageManagerDB._make_column(self, cls, key)
        if col.autoincrement and not sqlite._autoincrement_support:
            col.autoincrement = False
            col.initial = 0
        return col

