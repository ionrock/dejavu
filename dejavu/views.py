"""OLAP Views and Snapshots (materialized views).

Notice in particular that Snapshot and View are _temporary_ Units.
Even when you memorize them, they won't be persistent unless you set
each instance's Expiration to None.
"""

import datetime
import dejavu
from dejavu import errors, recur


class TemporaryUnit(dejavu.Unit):
    
    Expiration = dejavu.UnitProperty(datetime.datetime)
    
    def on_recall(self):
        if self.Expiration is not None:
            if self.Expiration <= datetime.datetime.now():
                self.forget()
                raise errors.UnrecallableError
            else:
                self.decay(minutes=15)
    
    def decay(self, **kw):
        """decay(**kw) -> Set Expiration to now() + timedelta(**kw)."""
        self.Expiration = datetime.datetime.now() + datetime.timedelta(**kw)


class TemporarySweeper(recur.Worker):
    """A worker to sweep out expired TemporaryUnit's."""
    
    def work(self):
        """Start a cycle of scheduled work."""
        now = datetime.datetime.now()
        f = lambda x: x.Expiration != None and x.Expiration <= now
        box = self.store.new_sandbox()
        
        for cls in self.store.classes:
            if issubclass(cls, TemporaryUnit):
                # Running box.recall will call TemporaryUnit.on_recall,
                # which should forget expired units.
                box.recall(cls, f)
        box.flush_all()


class View(TemporaryUnit):
    """A selector from (possibly multiple and/or aggregated) relations.
    
    When a View is called, it returns a Snapshot.
    """
    
    Name = dejavu.UnitProperty()
    Created = dejavu.UnitProperty(datetime.datetime)
    
    # The following are equivalent to:
    # "SELECT Attributes FROM Relation WHERE Restriction"
    Query = dejavu.UnitProperty(dejavu.Query)
    
    def __init__(self, Query=None, Created=None, **kwargs):
        if Created is None:
            Created = datetime.datetime.today()
        if not isinstance(Query, dejavu.Query):
            Query = dejavu.Query(*Query)
        TemporaryUnit.__init__(self, Query=Query, Created=Created, **kwargs)
    
    def on_forget(self):
        # Snapshots shouldn't persist past the life of their View.
        for mv in self.Snapshot():
            mv.forget()
    
    def __call__(self, name, store=None):
        """Execute self and return a Snapshot."""
        if store is None:
            store = self.sandbox.store
        newcls = store.insert_into(name, self.Query)
        snap = Snapshot(ViewID=self.ID, ResultName=name)
        snap.decay(minutes=15)
        return snap
    results = __call__
    
    def __copy__(self):
        view = TemporaryUnit.__copy__(self)
        view.Name = "Copy of %s" % self.Name
        view.Created = datetime.datetime.now()
        return view


class Snapshot(TemporaryUnit):
    """A materialized view; the result set obtained by calling a View.
    
    Each Snapshot Unit instance possesses its own additional, dynamic
    unit class which contains the actual result data. The 'ResultName'
    attribute of the Snapshot must match the class name of the actual
    result data.
    """
    
    ViewID = dejavu.UnitProperty(int, index=True)
    Timestamp = dejavu.UnitProperty(datetime.datetime)
    ResultName = dejavu.UnitProperty()
    
    def __init__(self, Timestamp=None, **kwargs):
        if Timestamp is None:
            Timestamp = datetime.datetime.now()
        TemporaryUnit.__init__(self, Timestamp=Timestamp, **kwargs)
    
    def unitclass(self, store=None):
        if store is None:
            store = self.sandbox.store
        try:
            return store.class_by_name(self.ResultName)
        except KeyError:
            newclass = store.make_class(self.ResultName)
            store.register(newclass)
            return newclass


View.one_to_many('ID', Snapshot, 'ViewID')


def register_classes(store):
    store.register(View)
    store.register(Snapshot)

