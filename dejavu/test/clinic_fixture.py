"""OLAP Test fixture for Storage Managers."""

import datetime
dt = datetime.datetime
td = datetime.timedelta
import os
thisdir = os.path.dirname(__file__)
logname = os.path.join(thisdir, "olaptest.log")

import sys
import traceback
import unittest

import dejavu
from dejavu import Unit, UnitProperty
from dejavu.test import tools
from dejavu import views


class Clinic(Unit):
    Name = UnitProperty()
    Type = UnitProperty()


class Argument(Unit):
    Topic = UnitProperty()
    Start = UnitProperty(dt)
    Duration = UnitProperty(datetime.timedelta)
    ClinicID = UnitProperty(int, index=True)

Argument.many_to_one('ClinicID', Clinic, 'ID')


class Participant(Unit):
    DirectoryID = UnitProperty(int, index=True)
    ArgumentID = UnitProperty(int, index=True)

Participant.many_to_one('ArgumentID', Argument, 'ID')


class Directory(Unit):
    Name = UnitProperty()
    Birth = UnitProperty(datetime.date)
    Gender = UnitProperty()

Directory.one_to_many('ID', Participant, 'DirectoryID')


class OLAPTests(unittest.TestCase):
    
    def test_1_populate(self):
        box = store.new_sandbox()
        
        c = Clinic(Name = 'The Argument Clinic')
        box.memorize(c)
        
        # Directory
        brown = Directory(Name='Sam Brown', Birth=datetime.date(1973, 3, 4),
                          Gender='Male')
        green = Directory(Name='David Green', Birth=datetime.date(1988, 11, 21),
                          Gender='Male')
        white = Directory(Name='Alice White', Birth=datetime.date(1964, 9, 8),
                          Gender='Female')
        for person in (brown, green, white):
            box.memorize(person)
        
        # Arguments
        sixtynine = dt(1969, 1, 1)
        for s in range(5):
            for d in range(12):
                arg = Argument(Topic = "What is an Argument?",
                               Start = sixtynine + td(s * 13.54 + d * 30),
                               Duration = td(minutes=d * 5),
                               ClinicID = c.ID,
                              )
                box.memorize(arg)
                box.memorize(Participant(DirectoryID=brown.ID, ArgumentID=arg.ID))
                if d % 2:
                    box.memorize(Participant(DirectoryID=green.ID, ArgumentID=arg.ID))
                else:
                    box.memorize(Participant(DirectoryID=white.ID, ArgumentID=arg.ID))
        box.flush_all()
    
    def test_2_simple_views(self):
        # Get the start and duration times for: "What is an Argument?"
        sixtynine = dt(1969, 1, 1)
        expected = []
        for s in range(5):
            for d in range(12):
                expected.append((sixtynine + td(s * 13.54 + d * 30),
                                 td(minutes=d * 5))
                                )
        expected.sort()
        
        # Make a new View.
        times = views.View((Argument, ['Start', 'Duration'],
                            lambda a: a.Topic == 'What is an Argument?'))
        
        # Take a new snapshot of that View.
        clsname = 'WhatIsAnArgumentTimes'
        snap = times.results(clsname, store)
        
        # Retrieve the Unit class for the Snapshot.
        snapclass = snap.unitclass(store)
        self.assertEqual(snapclass.__name__, clsname)
        
        # Pull the data from the Snapshot and assert it's as expected.
        actual = store.view((snapclass, ['Start', 'Duration']))
        actual.sort()
##        for a, e in zip(actual, expected):
##            if a != e:
##                print a, e
        self.assertEqual(actual, expected)
    
    def test_3_multidim_views(self):
        box = store.new_sandbox()
        brownid = box.unit(Directory, Name='Sam Brown').ID
        box.flush_all()
        
        brownargs = views.View((Clinic << Argument << Participant,
                                [['ID', 'Name'], None, None],
                                lambda c, a, p: p.DirectoryID == brownid))
        clsname = 'BrownClinics'
        snap = brownargs.results(clsname, store)
        snapclass = snap.unitclass(store)
        self.assertEqual(snapclass.__name__, clsname)
        actual = store.view((snapclass, ['Name']))
        actual.sort()
        self.assertEqual(actual, [(u'The Argument Clinic',)] * 60)
    
    def test_4_aggregate_views(self):
        firstargs = views.View((Argument << Participant << Directory,
                                lambda a, p, d: [d.Name, min(a.Start)]))
        clsname = 'FirstArgument'
        snap = firstargs.results(clsname, store)
        snapclass = snap.unitclass(store)
        self.assertEqual(snapclass.__name__, clsname)
        actual = store.view((snapclass, ))
        actual.sort()
        self.assertEqual(actual, [(u'Alice White', dt(1969, 1, 1)),
                                  (u'David Green', dt(1969, 1, 31)),
                                  (u'Sam Brown', dt(1969, 1, 1)),
                                  ])


store = dejavu.VerticalPartitioner()

def _djvlog(message):
    """Dejavu logger (writes to error.log)."""
    if isinstance(message, unicode):
        message = message.encode('utf8')
    s = "%s %s" % (dt.now().isoformat(), message)
    f = open(logname, 'ab')
    f.write(s + '\n')
    f.close()

def init():
    global store
    store = dejavu.VerticalPartitioner()
    store.log = _djvlog
    store.logflags = (dejavu.logflags.ERROR + dejavu.logflags.SQL +
                      dejavu.logflags.IO)

def setup(SM_class, opts):
    """Set up storage for Clinic classes."""
    sm = store.add_store('test_clinic', SM_class, opts)
    v = getattr(sm, "version", None)
    if v:
        print v()
    sm.create_database()
    
    store.register_all(globals())
    views.register_classes(store)
    store.map_all('repair')

def teardown():
    """Tear down storage for Clinic classes."""
    for store in store.stores.values():
        try:
            store.drop_database()
        except (AttributeError, NotImplementedError):
            pass
    store.stores = {}
    store.shutdown()

def run(SM_class, opts):
    """Run the clinic fixture."""
    try:
        try:
            setup(SM_class, opts)
            loader = unittest.TestLoader().loadTestsFromTestCase
            
            # Run the OLAPTests and time it.
            case = loader(OLAPTests)
            startTime = dt.now()
            tools.djvTestRunner.run(case)
            print "Ran clinic case in:", dt.now() - startTime
        except:
            traceback.print_exc()
    finally:
        teardown()
