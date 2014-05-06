"""Test fixture for Storage Managers."""

import datetime
import math

try:
    import pythoncom
except ImportError:
    pythoncom = None

import random

try:
    set
except NameError:
    from sets import Set as set

import sys
import threading
import time
import traceback
import unittest
import warnings

try:
    # Builtin in Python 2.5?
    decimal
except NameError:
    try:
        # Module in Python 2.3, 2.4
        import decimal
    except ImportError:
        decimal = None

try:
    import fixedpoint
except ImportError:
    fixedpoint = None

__all__ = ['Animal', 'Exhibit', 'Lecture', 'Vet', 'Visit', 'Zoo',
           # Don't export the ZooTests class--it will break e.g. test_dejavu.
           'root', 'run', 'setup', 'teardown']


import dejavu
from dejavu import errors, storage
from dejavu import Unit, UnitProperty, ToOne, ToMany, UnitSequencerInteger, UnitAssociation
from dejavu.test import tools
from dejavu import engines
from geniusql import logic, logicfuncs
logicfuncs.init()


class EscapeProperty(UnitProperty):
    def __set__(self, unit, value):
        UnitProperty.__set__(self, unit, value)
        # Zoo is a ToOne association, so it will return a unit or None.
        z = unit.Zoo()
        if z:
            z.LastEscape = unit.LastEscape


class Animal(Unit):
    Species = UnitProperty(hints={'bytes': 100})
    ZooID = UnitProperty(int, index=True)
    Legs = UnitProperty(int, default=4)
    PreviousZoos = UnitProperty(list, hints={'bytes': 8000})
    LastEscape = EscapeProperty(datetime.datetime)
    Lifespan = UnitProperty(float, hints={'precision': 4})
    Age = UnitProperty(float, hints={'precision': 4}, default=1)
    MotherID = UnitProperty(int)
    PreferredFoodID = UnitProperty(int)
    AlternateFoodID = UnitProperty(int)

Animal.many_to_one('ID', Animal, 'MotherID')


class Zoo(Unit):
    Name = UnitProperty()
    Founded = UnitProperty(datetime.date)
    Opens = UnitProperty(datetime.time)
    LastEscape = UnitProperty(datetime.datetime)
    
    if fixedpoint:
        # Explicitly set precision and scale so test_storemsaccess
        # can test CURRENCY type
        Admission = UnitProperty(fixedpoint.FixedPoint,
                                 hints={'precision': 4, 'scale': 2})
    else:
        Admission = UnitProperty(float)

Zoo.one_to_many('ID', Animal, 'ZooID')

class AlternateFoodAssociation(UnitAssociation):
    to_many = False
    register = False
    
    def related(self, unit, expr=None):
        food = unit.sandbox.unit(Food, ID=unit.AlternateFoodID)
        return food

class Food(Unit):
    """A food item."""
    Name = UnitProperty()
    NutritionValue = UnitProperty(int)

Food.one_to_many('ID', Animal, 'PreferredFoodID')

descriptor = AlternateFoodAssociation('AlternateFoodID', Food, 'ID')
descriptor.nearClass = Animal
Animal._associations['Alternate Food'] = descriptor
Animal.AlternateFood = descriptor
del descriptor

class Vet(Unit):
    """A Veterinarian."""
    Name = UnitProperty()
    ZooID = UnitProperty(int, index=True)
    FavoriteColor = UnitProperty()
    sequencer = UnitSequencerInteger(initial=200)

Vet.many_to_one('ZooID', Zoo, 'ID')


class Visit(Unit):
    """Work done by a Veterinarian on an Animal."""
    VetID = UnitProperty(int, index=True)
    ZooID = UnitProperty(int, index=True)
    AnimalID = UnitProperty(int, index=True)
    Date = UnitProperty(datetime.date)

Vet.one_to_many('ID', Visit, 'VetID')
Animal.one_to_many('ID', Visit, 'AnimalID')


class Lecture(Visit):
    """A Visit by a Vet to train staff (rather than visit an Animal)."""
    AnimalID = None
    Topic = UnitProperty()


class Exhibit(Unit):
    # Make this a string to help test vs unicode.
    Name = UnitProperty(str)
    ZooID = UnitProperty(int)
    Animals = UnitProperty(list)
    PettingAllowed = UnitProperty(bool)
    Creators = UnitProperty(tuple)
    
    if decimal:
        Acreage = UnitProperty(decimal.Decimal)
    else:
        Acreage = UnitProperty(float)
    
    # Remove the ID property (inherited from Unit) from the Exhibit class.
    ID = None
    sequencer = dejavu.UnitSequencer()
    identifiers = ("ZooID", Name)

Zoo.one_to_many('ID', Exhibit, 'ZooID')


class GateAccessLog(Unit):
    Timestamp = UnitProperty(datetime.datetime)
    CardID = UnitProperty(int)
    # This unit has no primary key
    ID = None
    identifiers = ()

May_16_2007 = datetime.datetime(2007, 5, 16)
logtimes = [(cardid, May_16_2007 + datetime.timedelta(0, random.randint(0, 86399)))
            for cardid in xrange(5) for x in xrange(5)]
logtimes.sort()
del cardid, x


class Ticket(Unit):
    """A sold admission ticket to the Zoo."""
    ZooID = UnitProperty(int, index=True)
    Price = UnitProperty(float)
    Date = UnitProperty(datetime.date)


class NothingToDoWithZoos(Unit):
    ALong = UnitProperty(long, hints={'precision': 1})
    AFloat = UnitProperty(float, hints={'precision': 1})
    if decimal:
        ADecimal = UnitProperty(decimal.Decimal,
                                hints={'precision': 1, 'scale': 1})
    if fixedpoint:
        AFixed = UnitProperty(fixedpoint.FixedPoint,
                              hints={'precision': 1, 'scale': 1})


Jan_1_2001 = datetime.date(2001, 1, 1)
every13days = [Jan_1_2001 + datetime.timedelta(x * 13) for x in range(20)]
every17days = [Jan_1_2001 + datetime.timedelta(x * 17) for x in range(20)]
del x

class ZooTests(unittest.TestCase):
    
    def test_1_model(self):
        self.assertEqual(Zoo.Animal.__class__, dejavu.ToMany)
        self.assertEqual(Zoo.Animal.nearClass, Zoo)
        self.assertEqual(Zoo.Animal.nearKey, 'ID')
        self.assertEqual(Zoo.Animal.farClass, Animal)
        self.assertEqual(Zoo.Animal.farKey, 'ZooID')
        
        self.assertEqual(Animal.Zoo.__class__, dejavu.ToOne)
        self.assertEqual(Animal.Zoo.nearClass, Animal)
        self.assertEqual(Animal.Zoo.nearKey, 'ZooID')
        self.assertEqual(Animal.Zoo.farClass, Zoo)
        self.assertEqual(Animal.Zoo.farKey, 'ID')
    
    def test_2_populate(self):
        box = root.new_sandbox()
        try:
            
            # Notice this also tests that: a Unit which is only
            # dirtied via __init__ is still saved.
            WAP = Zoo(Name = 'Wild Animal Park',
                      Founded = datetime.date(2000, 1, 1),
                      # 59 can give rounding errors with divmod, which
                      # ADO adapters needs to correct.
                      Opens = datetime.time(8, 15, 59),
                      LastEscape = datetime.datetime(2004, 7, 29, 5, 6, 7),
                      Admission = "4.95",
                      )
            box.memorize(WAP)
            # The object should get an ID automatically.
            self.assertNotEqual(WAP.ID, None)
            
            SDZ = Zoo(Name = 'San Diego Zoo',
                      # This early date should play havoc with a number
                      # of implementations.
                      Founded = datetime.date(1835, 9, 13),
                      Opens = datetime.time(9, 0, 0),
                      Admission = "0",
                      )
            box.memorize(SDZ)
            # The object should get an ID automatically.
            self.assertNotEqual(SDZ.ID, None)
            
            Biodome = Zoo(Name = u'Montr\xe9al Biod\xf4me',
                          Founded = datetime.date(1992, 6, 19),
                          Opens = datetime.time(9, 0, 0),
                          Admission = "11.75",
                          )
            box.memorize(Biodome)
            
            seaworld = Zoo(Name = 'Sea_World', Admission = "60")
            box.memorize(seaworld)
    ##        
    ##        mostly_empty = Zoo(Name = 'The Mostly Empty Zoo' + (" " * 255))
    ##        box.memorize(mostly_empty)
            
            # Animals
            leopard = Animal(Species='Leopard', Lifespan=73.5)
            self.assertEqual(leopard.PreviousZoos, None)
            box.memorize(leopard)
            self.assertEqual(leopard.ID, 1)
            
            leopard.add(WAP)
            leopard.LastEscape = datetime.datetime(2004, 12, 21, 8, 15, 0, 999907)
            
            lion = Animal(Species='Lion', ZooID=WAP.ID,
                          LastEscape = datetime.datetime(2007, 9, 24,
                                                         16, 18, 42))
            box.memorize(lion)
            
            box.memorize(Animal(Species='Slug', Legs=1, Lifespan=.75,
                                # Test our 8000-byte limit
                                PreviousZoos=["f" * (8000 - 14)]))
            
            tiger = Animal(Species='Tiger', PreviousZoos=['animal\\universe'])
            box.memorize(tiger)
            
            # Override Legs.default with itself just to make sure it works.
            box.memorize(Animal(Species='Bear', Legs=4))
            # Notice that ostrich.PreviousZoos is [], whereas leopard is None.
            box.memorize(Animal(Species='Ostrich', Legs=2, PreviousZoos=[],
                                Lifespan=103.2))
            box.memorize(Animal(Species='Centipede', Legs=100))
            
            emp = Animal(Species='Emperor Penguin', Legs=2)
            box.memorize(emp)
            adelie = Animal(Species='Adelie Penguin', Legs=2,
                            LastEscape = datetime.datetime(2007, 9, 20,
                                                           19, 10, 14))
            box.memorize(adelie)
            
            seaworld.add(emp, adelie)
            
            millipede = Animal(Species='Millipede', Legs=1000000)
            millipede.PreviousZoos = [WAP.Name]
            box.memorize(millipede)
            
            SDZ.add(tiger, millipede)
            
            # Add a mother and child to test relationships
            bai_yun = Animal(Species='Ape', Legs=2)
            box.memorize(bai_yun)   # ID = 11
            self.assertEqual(bai_yun.ID, 11)
            hua_mei = Animal(Species='Ape', Legs=2, MotherID=bai_yun.ID)
            box.memorize(hua_mei)   # ID = 12
            self.assertEqual(hua_mei.ID, 12)
            
            # Exhibits
            pe = Exhibit(Name = 'The Penguin Encounter',
                         ZooID = seaworld.ID,
                         Animals = [emp.ID, adelie.ID],
                         PettingAllowed = True,
                         Acreage = "3.1",
                         # See ticket #45
                         Creators = (u'Richard F\xfcrst', u'Sonja Martin'),
                         )
            box.memorize(pe)
            
            tr = Exhibit(Name = 'Tiger River',
                         ZooID = SDZ.ID,
                         Animals = [tiger.ID],
                         PettingAllowed = False,
                         Acreage = "4",
                         )
            box.memorize(tr)
            
            # Vets
            cs = Vet(Name = 'Charles Schroeder', ZooID = SDZ.ID,
                     FavoriteColor = 'Red')
            box.memorize(cs)
            self.assertEqual(cs.ID, Vet.sequencer.initial)
            
            jm = Vet(Name = 'Jim McBain', ZooID = seaworld.ID,
                     FavoriteColor = 'Red')
            box.memorize(jm)
            
            # Visits
            for d in every13days:
                box.memorize(Visit(VetID=cs.ID, AnimalID=tiger.ID, Date=d))
            for d in every17days:
                box.memorize(Visit(VetID=jm.ID, AnimalID=emp.ID, Date=d))
            
            # Foods
            dead_fish = Food(Name="Dead Fish", Nutrition=5)
            live_fish = Food(Name="Live Fish", Nutrition=10)
            bunnies = Food(Name="Live Bunny Wabbit", Nutrition=10)
            steak = Food(Name="T-Bone", Nutrition=7)
            for food in [dead_fish, live_fish, bunnies, steak]:
                box.memorize(food)
            
            # Foods --> add preferred foods
            lion.add(steak)
            tiger.add(bunnies)
            emp.add(live_fish)
            adelie.add(live_fish)
            
            # Foods --> add alternate foods
            lion.AlternateFoodID = bunnies.ID
            tiger.AlternateFoodID = steak.ID
            emp.AlternateFoodID = dead_fish.ID
            adelie.AlternateFoodID = dead_fish.ID
            
            # GateAccessLog (no identity!)
            for cardid, t in logtimes:
                box.memorize(GateAccessLog(Timestamp=t, CardID=cardid))
        finally:
            box.flush_all()
    
    def test_3_Properties(self):
        box = root.new_sandbox()
        try:
            # Zoos
            WAP = box.unit(Zoo, Name='Wild Animal Park')
            self.assertNotEqual(WAP, None)
            self.assertEqual(WAP.Founded, datetime.date(2000, 1, 1))
            self.assertEqual(WAP.Opens, datetime.time(8, 15, 59))
            # This should have been updated when leopard.LastEscape was set.
    ##        self.assertEqual(WAP.LastEscape,
    ##                         datetime.datetime(2004, 12, 21, 8, 15, 0, 999907))
            self.assertEqual(WAP.Admission, Zoo.Admission.coerce(WAP, "4.95"))
            
            SDZ = box.unit(Zoo, Founded=datetime.date(1835, 9, 13))
            self.assertNotEqual(SDZ, None)
            self.assertEqual(SDZ.Founded, datetime.date(1835, 9, 13))
            self.assertEqual(SDZ.Opens, datetime.time(9, 0, 0))
            self.assertEqual(SDZ.LastEscape, None)
            self.assertEqual(float(SDZ.Admission), 0)
            
            # Try a magic Sandbox recaller method
            Biodome = box.Zoo(Name = u'Montr\xe9al Biod\xf4me')
            self.assertNotEqual(Biodome, None)
            self.assertEqual(Biodome.Name, u'Montr\xe9al Biod\xf4me')
            self.assertEqual(Biodome.Founded, datetime.date(1992, 6, 19))
            self.assertEqual(Biodome.Opens, datetime.time(9, 0, 0))
            self.assertEqual(Biodome.LastEscape, None)
            self.assertEqual(float(Biodome.Admission), 11.75)
            
            if fixedpoint:
                seaworld = box.unit(Zoo, Admission = fixedpoint.FixedPoint(60))
            else:
                seaworld = box.unit(Zoo, Admission = float(60))
            self.assertNotEqual(seaworld, None)
            self.assertEqual(seaworld.Name, u'Sea_World')
            
            # Animals
            leopard = box.unit(Animal, Species='Leopard')
            self.assertEqual(leopard.Species, 'Leopard')
            self.assertEqual(leopard.Legs, 4)
            self.assertEqual(leopard.Lifespan, 73.5)
            self.assertEqual(leopard.ZooID, WAP.ID)
            self.assertEqual(leopard.PreviousZoos, None)
    ##        self.assertEqual(leopard.LastEscape,
    ##                         datetime.datetime(2004, 12, 21, 8, 15, 0, 999907))
            
            ostrich = box.unit(Animal, Species='Ostrich')
            self.assertEqual(ostrich.Species, 'Ostrich')
            self.assertEqual(ostrich.Legs, 2)
            self.assertEqual(ostrich.ZooID, None)
            self.assertEqual(ostrich.PreviousZoos, [])
            self.assertEqual(ostrich.LastEscape, None)
            
            millipede = box.unit(Animal, Legs=1000000)
            self.assertEqual(millipede.Species, 'Millipede')
            self.assertEqual(millipede.Legs, 1000000)
            self.assertEqual(millipede.ZooID, SDZ.ID)
            self.assertEqual(millipede.PreviousZoos, [WAP.Name])
            self.assertEqual(millipede.LastEscape, None)
            
            # Test that strings in a list get decoded correctly.
            # See http://projects.amor.org/dejavu/ticket/50
            tiger = box.unit(Animal, Species='Tiger')
            self.assertEqual(tiger.PreviousZoos, ["animal\\universe"])
            
            # Test our 8000-byte limit.
            # len(pickle.dumps(["f" * (8000 - 14)]) == 8000
            slug = box.unit(Animal, Species='Slug')
            self.assertEqual(len(slug.PreviousZoos[0]), 8000 - 14)
            
            # Exhibits
            exes = box.recall(Exhibit)
            self.assertEqual(len(exes), 2)
            if exes[0].Name == 'The Penguin Encounter':
                pe = exes[0]
                tr = exes[1]
            else:
                pe = exes[1]
                tr = exes[0]
            self.assertEqual(pe.ZooID, seaworld.ID)
            self.assertEqual(len(pe.Animals), 2)
            self.assertEqual(float(pe.Acreage), 3.1)
            self.assertEqual(pe.PettingAllowed, True)
            self.assertEqual(pe.Creators, (u'Richard F\xfcrst', u'Sonja Martin'))
            
            self.assertEqual(tr.ZooID, SDZ.ID)
            self.assertEqual(len(tr.Animals), 1)
            self.assertEqual(float(tr.Acreage), 4)
            self.assertEqual(tr.PettingAllowed, False)
            
        finally:
            box.flush_all()
    
    def test_4_Expressions(self):
        box = root.new_sandbox()
        try:
            def matches(lam, cls=Animal):
                # We flush_all to ensure a DB hit each time.
                box.flush_all()
                return len(box.recall(cls, lam))
            
            zoos = box.recall(Zoo)
            self.assertEqual(zoos[0].dirty(), False)
            self.assertEqual(len(zoos), 4)
            self.assertEqual(matches(lambda x: True), 12)
            self.assertEqual(matches(lambda x: x.Legs == 4), 4)
            self.assertEqual(matches(lambda x: x.Legs == 2), 5)
            self.assertEqual(matches(lambda x: x.Legs >= 2 and x.Legs < 20), 9)
            self.assertEqual(matches(lambda x: x.Legs > 10), 2)
            self.assertEqual(matches(lambda x: x.Lifespan > 70), 2)
            self.assertEqual(matches(lambda x: x.Species.startswith('L')), 2)
            self.assertEqual(matches(lambda x: x.Species.endswith('pede')), 2)
            self.assertEqual(matches(lambda x: x.LastEscape != None), 3)
            self.assertEqual(matches(lambda x: x.LastEscape is not None), 3)
            self.assertEqual(matches(lambda x: None == x.LastEscape), 9)
            
            # In operator (containedby)
            self.assertEqual(matches(lambda x: 'pede' in x.Species), 2)
            self.assertEqual(matches(lambda x: x.Species in ('Lion', 'Tiger', 'Bear')), 3)
            
            # Try In with cell references
            class thing(object): pass
            pet, pet2 = thing(), thing()
            pet.Name, pet2.Name = 'Slug', 'Ostrich'
            self.assertEqual(matches(lambda x: x.Species in (pet.Name, pet2.Name)), 2)
            
            # logic and other functions
            self.assertEqual(matches(lambda x: ieq(x.Species, 'slug')), 1)
            self.assertEqual(matches(lambda x: icontains(x.Species, 'PEDE')), 2)
            self.assertEqual(matches(lambda x: icontains(('Lion', 'Banana'), x.Species)), 1)
            f = lambda x: icontainedby(x.Species, ('Lion', 'Bear', 'Leopard'))
            self.assertEqual(matches(f), 3)
            name = 'Lion'
            self.assertEqual(matches(lambda x: len(x.Species) == len(name)), 3)
            
            # This broke sometime in 2004. Rev 32 seems to have fixed it.
            self.assertEqual(matches(lambda x: 'i' in x.Species), 7)
            
            # Test now(), today(), year(), month(), day()
            self.assertEqual(matches(lambda x: x.Founded != None
                                     and x.Founded < today(), Zoo), 3)
            self.assertEqual(matches(lambda x: x.LastEscape == now()), 0)
            self.assertEqual(matches(lambda x: year(x.LastEscape) == 2004), 1)
            self.assertEqual(matches(lambda x: month(x.LastEscape) == 12), 1)
            self.assertEqual(matches(lambda x: day(x.LastEscape) == 21), 1)

            
            # Test AND, OR with cannot_represent.
            # Notice that we reference a method ('count') which no
            # known SM handles, so it will default back to Expr.eval().
            self.assertEqual(matches(lambda x: 'p' in x.Species
                                     and x.Species.count('e') > 1), 3)
            
            # This broke in MSAccess (storeado) in April 2005, due to a bug in
            # db.SQLDecompiler.visit_CALL_FUNCTION (append TOS, not replace!).
            box.flush_all()
            e = logic.Expression(lambda x, **kw: x.LastEscape != None
                                 and x.LastEscape >= datetime.datetime(kw['Year'], 12, 1)
                                 and x.LastEscape < datetime.datetime(kw['Year'], 12, 31)
                                 )
            e.bind_args(Year=2004)
            units = box.recall(Animal, e)
            self.assertEqual(len(units), 1)
            
            # Test wildcards in LIKE. This fails with SQLite <= 3.0.8,
            # so make sure it's always at the end of this method so
            # it doesn't preclude running the other tests.
            box.flush_all()
            units = box.recall(Zoo, lambda x: "_" in x.Name)
            self.assertEqual(len(units), 1)
        finally:
            box.flush_all()
    
    def test_5_Aggregates(self):
        box = root.new_sandbox()
        try:
            # views
            legs = [x[0] for x in box.view((Animal, ['Legs']))]
            legs.sort()
            self.assertEqual(legs, [1, 2, 2, 2, 2, 2, 4, 4, 4, 4, 100, 1000000])
            
            expected = {'Leopard': 73.5,
                        'Slug': .75,
                        'Tiger': None,
                        'Lion': None,
                        'Bear': None,
                        'Ostrich': 103.2,
                        'Centipede': None,
                        'Emperor Penguin': None,
                        'Adelie Penguin': None,
                        'Millipede': None,
                        'Ape': None,
                        }
            for species, lifespan in box.view((Animal, ['Species', 'Lifespan'])):
                if expected[species] is None:
                    self.assertEqual(lifespan, None)
                else:
                    self.assertAlmostEqual(expected[species], lifespan, places=5)
            
            expected = [u'Montr\xe9al Biod\xf4me', 'Wild Animal Park']
            e = (lambda x: x.Founded != None
                 and x.Founded <= today()
                 and x.Founded >= datetime.date(1990, 1, 1))
            values =  [val[0] for val in box.view((Zoo, ['Name'], e))]
            for name in expected:
                self.assert_(name in values)
            
            # distinct
            legs = box.view((Animal, ['Legs']), distinct=True)
            legs.sort()
            self.assertEqual(legs, [(1, ), (2, ), (4, ), (100, ), (1000000, )])
            
            # This may raise a warning on some DB's.
            f = (lambda x: x.Species == 'Lion')
            escapees = box.view((Animal, ['Legs'], f), distinct=True)
            self.assertEqual(escapees, [(4, ), ])
            
            # count should use an aggregate function when using DB's
            fourlegged = root.count(Animal, lambda x: x.Legs == 4)
            self.assertEqual(fourlegged, 4)
            topics = root.count(Exhibit)
            self.assertEqual(topics, 2)
            
            # range should return a sorted list
            legs = box.range(Animal, 'Legs', lambda x: x.Legs <= 100)
            self.assertEqual(legs, range(1, 101))
            topics = box.range(Exhibit, 'Name')
            self.assertEqual(topics, ['The Penguin Encounter', 'Tiger River'])
            vets = box.range(Vet, 'Name')
            self.assertEqual(vets, ['Charles Schroeder', 'Jim McBain'])
            
            # Test view() with a Unit that has no identifiers (primary keys).
            access = box.view((GateAccessLog, ['CardID', 'Timestamp']))
            access.sort()
            self.assertEqual(len(access), len(logtimes))
            self.assertEqual(access, logtimes)
        finally:
            box.flush_all()
    
    def test_5a_order_and_limit(self):
        all_animals = [u'Adelie Penguin', u'Ape', u'Ape', u'Bear',
                       u'Centipede', u'Emperor Penguin', u'Leopard',
                       u'Lion', u'Millipede', u'Ostrich', u'Slug', u'Tiger']
        len_animals = len(all_animals)
        
        # Test various limit points against a sandbox.
        box = root.new_sandbox()
        try:
            animals = box.recall(Animal, order=['Species'])
            self.assertEqual([a.Species for a in animals], all_animals)
            for lim in range(len_animals + 1):
                animals = box.recall(Animal, order=['Species'], limit=lim)
                self.assertEqual([a.Species for a in animals], all_animals[:lim])
        finally:
            box.flush_all()
        
        # Test various limit points against root.
        animals = root.recall(Animal, order=['Species'])
        self.assertEqual([a.Species for a in animals], all_animals)
        for lim in range(len_animals + 1):
            animals = root.recall(Animal, order=['Species'], limit=lim)
            self.assertEqual([a.Species for a in animals], all_animals[:lim])
        
        animals = root.recall(Animal, order=['Species'], limit=5, offset=2)
        self.assertEqual([a.Species for a in animals],
                         [u'Ape', u'Bear', u'Centipede',
                          u'Emperor Penguin', u'Leopard'])
        
        animals = root.view((Animal, ['Legs', 'Species'], None),
                            order=['Species'], limit=5, offset=2)
        self.assertEqual(animals, [(2, u'Ape'), (4, u'Bear'),
                                   (100, u'Centipede'),
                                   (2, u'Emperor Penguin'),
                                   (4, u'Leopard')])
        
        # Test reversed()
        zoos = root.recall(Zoo, lambda z: z.Founded != None,
                           order=lambda z: [reversed(z.Founded)],
                           limit=2,
                           )
        self.assertEqual([z.Founded for z in zoos],
                         [datetime.date(2000, 1, 1),
                          datetime.date(1992, 6, 19),
                          ])
        
        # Test limit, reversed() with a join.
        data = root.recall(Animal << Zoo, lambda a, z: a.LastEscape != None,
                           order=lambda a, z: [reversed(a.LastEscape)],
                           limit=1)
        # If we didn't limit, we should have received:
        # [datetime.datetime(2007, 9, 24, 16, 18, 42),
        #  datetime.datetime(2007, 9, 20, 19, 10, 14),
        #  datetime.datetime(2004, 12, 21, 8, 15),
        #  ]
        self.assertEqual([a.LastEscape for a, z in data],
                         [datetime.datetime(2007, 9, 24, 16, 18, 42)])
        
        # Try ordering using the 'related units' method.
        box = root.new_sandbox()
        try:
            wap = box.unit(Zoo, Name='Wild Animal Park')
            data = wap.Animal(lambda a: a.LastEscape != None,
                              order=lambda a: [reversed(a.LastEscape)],
                              limit=2)
            self.assertEqual([a.LastEscape.replace(microsecond=0) for a in data],
                             [datetime.datetime(2007, 9, 24, 16, 18, 42),
                              datetime.datetime(2004, 12, 21, 8, 15),
                              ])
        finally:
            box.flush_all()
        
        # Test that offset with no order raises an error.
        try:
            root.recall(Animal, offset=3)
        except:
            pass
        else:
            self.fail("offset with no order did not raise an error.")
        try:
            root.new_sandbox().recall(Animal, offset=3)
        except:
            pass
        else:
            self.fail("offset with no order did not raise an error.")
    
    def test_6_Editing(self):
        # Edit
        box = root.new_sandbox()
        try:
            SDZ = box.unit(Zoo, Name='San Diego Zoo')
            SDZ.Name = 'The San Diego Zoo'
            SDZ.Founded = datetime.date(1900, 1, 1)
            SDZ.Opens = datetime.time(7, 30, 0)
            SDZ.Admission = "35.00"
            
            # Test that unit/recall get the sandboxed values (not storage).
            SDZ = box.unit(Zoo, Name='The San Diego Zoo')
            self.assertEqual(SDZ.Name, 'The San Diego Zoo')
            self.assertEqual(SDZ.Founded, datetime.date(1900, 1, 1))
            self.assertEqual(SDZ.Opens, datetime.time(7, 30, 0))
            
            SDZ = box.recall(Zoo, lambda z: z.Name == 'The San Diego Zoo')[0]
            self.assertEqual(SDZ.Name, 'The San Diego Zoo')
            self.assertEqual(SDZ.Founded, datetime.date(1900, 1, 1))
            self.assertEqual(SDZ.Opens, datetime.time(7, 30, 0))
        finally:
            box.flush_all()
        
        # Test edits
        box = root.new_sandbox()
        try:
            SDZ = box.unit(Zoo, Name='The San Diego Zoo')
            self.assertEqual(SDZ.Name, 'The San Diego Zoo')
            self.assertEqual(SDZ.Founded, datetime.date(1900, 1, 1))
            self.assertEqual(SDZ.Opens, datetime.time(7, 30, 0))
            if fixedpoint:
                self.assertEqual(SDZ.Admission, fixedpoint.FixedPoint(35, 2))
            else:
                self.assertEqual(SDZ.Admission, 35.0)
        finally:
            box.flush_all()
        
        # Change it back
        box = root.new_sandbox()
        try:
            SDZ = box.unit(Zoo, Name='The San Diego Zoo')
            SDZ.Name = 'San Diego Zoo'
            SDZ.Founded = datetime.date(1835, 9, 13)
            SDZ.Opens = datetime.time(9, 0, 0)
            SDZ.Admission = "0"
        finally:
            box.flush_all()
        
        # Test re-edits
        box = root.new_sandbox()
        try:
            SDZ = box.unit(Zoo, Name='San Diego Zoo')
            self.assertEqual(SDZ.Name, 'San Diego Zoo')
            self.assertEqual(SDZ.Founded, datetime.date(1835, 9, 13))
            self.assertEqual(SDZ.Opens, datetime.time(9, 0, 0))
            if fixedpoint:
                self.assertEqual(SDZ.Admission, fixedpoint.FixedPoint(0, 2))
            else:
                self.assertEqual(SDZ.Admission, 0.0)
        finally:
            box.flush_all()
    
    def test_7_Multirecall(self):
        box = root.new_sandbox()
        try:
            f = (lambda z, a: z.Name == 'San Diego Zoo')
            zooed_animals = box.recall(Zoo & Animal, f)
            self.assertEqual(len(zooed_animals), 2)
            
            SDZ = box.unit(Zoo, Name='San Diego Zoo')
            aid = 0
            for z, a in zooed_animals:
                self.assertEqual(id(z), id(SDZ))
                self.assertNotEqual(id(a), aid)
                aid = id(a)
            
            # Assert that multirecalls with no matching related units returns
            # no matches for the initial class, since all joins are INNER.
            # We're also going to test that you can combine a one-arg expr
            # with a two-arg expr.
            sdexpr = logic.filter(Name='San Diego Zoo')
            leo = lambda z, a: a.Species == 'Leopard'
            zooed_animals = box.recall(Zoo & Animal, sdexpr + leo)
            self.assertEqual(len(zooed_animals), 0)
            
            # Now try the same expr with INNER, LEFT, and RIGHT JOINs.
            zooed_animals = box.recall(Zoo & Animal)
            self.assertEqual(len(zooed_animals), 6)
            self.assertEqual(set([(z.Name, a.Species) for z, a in zooed_animals]),
                             set([("Wild Animal Park", "Leopard"),
                                  ("Wild Animal Park", "Lion"),
                                  ("San Diego Zoo", "Tiger"),
                                  ("San Diego Zoo", "Millipede"),
                                  ("Sea_World", "Emperor Penguin"),
                                  ("Sea_World", "Adelie Penguin")]))
            
            zooed_animals = box.recall(Zoo >> Animal)
            self.assertEqual(len(zooed_animals), 12)
            self.assertEqual(set([(z.Name, a.Species) for z, a in zooed_animals]),
                             set([("Wild Animal Park", "Leopard"),
                                  ("Wild Animal Park", "Lion"),
                                  ("San Diego Zoo", "Tiger"),
                                  ("San Diego Zoo", "Millipede"),
                                  ("Sea_World", "Emperor Penguin"),
                                  ("Sea_World", "Adelie Penguin"),
                                  (None, "Slug"),
                                  (None, "Bear"),
                                  (None, "Ostrich"),
                                  (None, "Centipede"),
                                  (None, "Ape"),
                                  (None, "Ape"),
                                  ]))
            
            zooed_animals = box.recall(Zoo << Animal)
            self.assertEqual(len(zooed_animals), 7)
            self.assertEqual(set([(z.Name, a.Species) for z, a in zooed_animals]),
                             set([("Wild Animal Park", "Leopard"),
                                  ("Wild Animal Park", "Lion"),
                                  ("San Diego Zoo", "Tiger"),
                                  ("San Diego Zoo", "Millipede"),
                                  ("Sea_World", "Emperor Penguin"),
                                  ("Sea_World", "Adelie Penguin"),
                                  (u'Montr\xe9al Biod\xf4me', None),
                                  ]))
            
            # Try a multiple-arg expression
            f = (lambda a, z: a.Legs >= 4 and z.Admission < 10)
            animal_zoos = box.recall(Animal & Zoo, f)
            self.assertEqual(len(animal_zoos), 4)
            names = [a.Species for a, z in animal_zoos]
            names.sort()
            self.assertEqual(names, ['Leopard', 'Lion', 'Millipede', 'Tiger'])
            
            # Let's try three joined classes just for the sadistic fun of it.
            tree = (Animal >> Zoo) >> Vet
            f = (lambda a, z, v: z.Name == 'Sea_World')
            self.assertEqual(len(box.recall(tree, f)), 2)
            
            # MSAccess can't handle an INNER JOIN nested in an OUTER JOIN.
            # Test that this fails for MSAccess, but works for other SM's.
            trees = []
            def make_tree():
                trees.append( (Animal & Zoo) >> Vet )
            warnings.filterwarnings("ignore", category=errors.StorageWarning)
            try:
                make_tree()
            finally:
                warnings.filters.pop(0)
            
            azv = []
            def set_azv():
                f = (lambda a, z, v: z.Name == 'Sea_World')
                azv.append(box.recall(trees[0], f))
            
            try:
                set_azv()
            except pythoncom.com_error:
                # MSAccess should raise com_error
                warnings.warn("illegal INNER JOIN nested in OUTER JOIN.")
            else:
                self.assertEqual(len(azv[0]), 2)
            
            # Try mentioning the same class twice.
            tree = (Animal << Animal)
            f = (lambda anim, mother: mother.ID != None)
            animals = [mother.ID for anim, mother in box.recall(tree, f)]
            self.assertEqual(animals, [11])
        finally:
            box.flush_all()
    
    def test_8_CustomAssociations(self):
        box = root.new_sandbox()
        try:
            # Try different association paths
            std_expected = ['Live Bunny Wabbit', 'Live Fish', 'Live Fish', 'T-Bone']
            cus_expected = ['Dead Fish', 'Dead Fish', 'Live Bunny Wabbit', 'T-Bone']
            uj = Animal & Food
            for path, expected in [# standard path
                                   (None, std_expected),
                                   # custom path
                                   ('Alternate Food', cus_expected)]:
                
                uj.path = path
                foods = [food for animal, food in box.recall(uj)]
                foods.sort(dejavu.sort('Name'))
                self.assertEqual([f.Name for f in foods], expected)

            # Test the magic association methods
            tiger = box.unit(Animal, Species='Tiger')
            self.assertEqual(tiger.Food().Name, 'Live Bunny Wabbit')
            self.assertEqual(tiger.AlternateFood().Name, 'T-Bone')
            
        finally:
            box.flush_all()
    
    def test_9_store_views(self):
        # Test a simple view
        legs = [x[0] for x in root.view((Animal, ['Legs']))]
        legs.sort()
        self.assertEqual(legs, [1, 2, 2, 2, 2, 2, 4, 4, 4, 4, 100, 1000000])
        
        # Test multiple columns
        expected = {'Leopard': 73.5,
                    'Slug': .75,
                    'Tiger': None,
                    'Lion': None,
                    'Bear': None,
                    'Ostrich': 103.2,
                    'Centipede': None,
                    'Emperor Penguin': None,
                    'Adelie Penguin': None,
                    'Millipede': None,
                    'Ape': None,
                    }
        for species, lifespan in root.view((Animal, ['Species', 'Lifespan'])):
            if expected[species] is None:
                self.assertEqual(lifespan, None)
            else:
                self.assertAlmostEqual(expected[species], lifespan, places=5)
        
        # Try a restriction Expression.
        expected = [u'Montr\xe9al Biod\xf4me', 'Wild Animal Park']
        e = (lambda x: x.Founded != None
             and x.Founded <= today()
             and x.Founded >= datetime.date(1990, 1, 1))
        values =  [val[0] for val in root.view((Zoo, ['Name'], e))]
        for name in expected:
            self.assert_(name in values)
        
        # distinct
        distinct_legs = [(1, ), (2, ), (4, ), (100, ), (1000000, )]
        legs = root.view((Animal, ['Legs']), distinct=True)
        legs.sort()
        self.assertEqual(legs, distinct_legs)
        legs = root.view((Animal, lambda a: (a.Legs,)), distinct=True)
        legs.sort()
        self.assertEqual(legs, distinct_legs)
        
        # This may raise a warning on some DB's.
        f = (lambda x: x.Species == 'Lion')
        escapees = root.view((Animal, ['Legs'], f), distinct=True)
        self.assertEqual(escapees, [(4, )])
        
        # range should return a sorted list
        legs = root.range(Animal, 'Legs', lambda x: x.Legs <= 100)
        self.assertEqual(legs, range(1, 101))
        topics = root.range(Exhibit, 'Name')
        self.assertEqual(topics, ['The Penguin Encounter', 'Tiger River'])
        vets = root.range(Vet, 'Name')
        self.assertEqual(vets, ['Charles Schroeder', 'Jim McBain'])
        
        # Now try views on a join.
        all_legs_and_names = [(1, None), (2, None), (2, None), (2, None),
                              (2, u'Sea_World'), (2, u'Sea_World'),
                              (4, None), (4, u'San Diego Zoo'),
                              (4, u'Wild Animal Park'),
                              (4, u'Wild Animal Park'),
                              (100, None), (1000000, u'San Diego Zoo'),
                              ]
        legs = root.view((Animal << Zoo, lambda a, z: (a.Legs, z.Name)))
        legs.sort()
        self.assertEqual(legs, all_legs_and_names)
        legs = root.view((Animal << Zoo, [("Legs",), ("Name",)]))
        legs.sort()
        self.assertEqual(legs, all_legs_and_names)
        
        # Altogether now: distinct view with restriction on a join.
        legs = root.view((Animal << Zoo,
                           lambda a, z: (a.Legs, z.Name),
                           lambda a, z: z.Name is not None and "o" in z.Name),
                          distinct=True)
        legs.sort()
        self.assertEqual(legs, [(2, u'Sea_World'),
                                (4, u'San Diego Zoo'),
                                (1000000, u'San Diego Zoo'),
                                ])
    
    def test_Sandbox_Sync(self):
        # This should expose all sorts of problems where data in the
        # Sandbox has changed (but hasn't been flushed).
        box = root.new_sandbox()
        try:
            # Restriction expression data out of sync.
            # This should work just fine for a single unit class.
            reds = box.recall(Vet, lambda v: v.FavoriteColor == 'Red')
            self.assertEqual(len(reds), 2)
            # Change one and try again
            aVet = reds[0]
            aVet.FavoriteColor = 'Blue'
            reds = box.recall(Vet, lambda v: v.FavoriteColor == 'Red')
            self.assertEqual(len(reds), 1)
            # Change it back and try again
            aVet.FavoriteColor = 'Red'
            reds = box.recall(Vet, lambda v: v.FavoriteColor == 'Red')
            self.assertEqual(len(reds), 2)
            
            # Restriction expression data out of sync.
            # This currently doesn't work for multiple classes.
            reds = box.recall(Zoo << Vet,
                              lambda z, v: (z.Name == 'San Diego Zoo' and
                                            v.FavoriteColor == 'Red'))
            self.assertEqual(len(reds), 1)
            # Change the color and try again
            aVet = reds[0][0]
            aVet.FavoriteColor = 'Blue'
            blues = box.recall(Zoo << Vet,
                               lambda z, v: (z.Name == 'San Diego Zoo' and
                                             v.FavoriteColor == 'Blue'))
            self.assertEqual(len(blues), 1)
            # Change it back and try again
            aVet.FavoriteColor = 'Red'
            reds = box.recall(Zoo << Vet,
                              lambda z, v: (z.Name == 'San Diego Zoo' and
                                            v.FavoriteColor == 'Red'))
            self.assertEqual(len(reds), 1)
            
            # Join keys out of sync.
            swpengs = box.recall(Zoo << Animal,
                                 lambda z, a: 'Penguin' in a.Species)
            self.assertEqual(len(swpengs), 2)    # emp, adelie
            # Move a penguin to another Zoo and try again.
            swpengs[0][1].ZooID = None
            swpengs = box.recall(Zoo << Animal,
                                 lambda z, a: 'Penguin' in a.Species)
            self.assertEqual(len(swpengs), 1)
        finally:
            box.flush_all()
    
    def test_Iteration(self):
        box = root.new_sandbox()
        try:
            # Test box.unit inside of xrecall
            for visit in box.xrecall(Visit, dict(VetID=1)):
                firstvisit = box.unit(Visit, VetID=1, Date=Jan_1_2001)
                self.assertEqual(firstvisit.VetID, 1)
                self.assertEqual(visit.VetID, 1)
            
            # Test recall inside of xrecall
            for visit in box.xrecall(Visit, dict(VetID=1)):
                f = (lambda x: x.VetID == 1 and x.ID != visit.ID)
                othervisits = box.recall(Visit, f)
                self.assertEqual(len(othervisits), len(every13days) - 1)
            
            # Test far associations inside of xrecall
            for visit in box.xrecall(Visit, dict(VetID=1)):
                # visit.Vet is a ToOne association, so will return a unit or None.
                vet = visit.Vet()
                self.assertEqual(vet.ID, 1)
        finally:
            box.flush_all()
    
    def test_Engines(self):
        box = root.new_sandbox()
        try:
            quadrupeds = box.recall(Animal, dict(Legs=4))
            self.assertEqual(len(quadrupeds), 4)
            
            eng = engines.UnitEngine()
            box.memorize(eng)
            eng.add_rule('CREATE', 1, "Animal")
            eng.add_rule('FILTER', 1, logic.filter(Legs=4))
            self.assertEqual(eng.FinalClassName, "Animal")
            
            qcoll = eng.take_snapshot()
            self.assertEqual(len(qcoll), 4)
            self.assertEqual(qcoll.EngineID, eng.ID)
            
            eng.add_rule('TRANSFORM', 1, "Zoo")
            self.assertEqual(eng.FinalClassName, "Zoo")
            
            # Sleep for a second so the Timestamps are different.
            time.sleep(1)
            qcoll = eng.take_snapshot()
            self.assertEqual(len(qcoll), 2)
            zoos = qcoll.units()
            zoos.sort(dejavu.sort('Name'))
            
            SDZ = box.unit(Zoo, Name='San Diego Zoo')
            WAP = box.unit(Zoo, Name='Wild Animal Park')
            self.assertEqual(zoos, [SDZ, WAP])
            
            # Flush and start over
            box.flush_all()
            box = root.new_sandbox()
            
            # Use the Sandbox magic recaller method
            eng = box.UnitEngine(1)
            self.assertEqual(len(eng.rules()), 3)
            snaps = eng.snapshots()
            self.assertEqual(len(snaps), 2)
            
            self.assertEqual(snaps[0].Type, "Animal")
            self.assertEqual(len(snaps[0]), 4)
            
            self.assertEqual(snaps[1].Type, "Zoo")
            self.assertEqual(len(snaps[1]), 2)
            self.assertEqual(eng.last_snapshot(), snaps[1])
            
            # Remove the last TRANSFORM rule to see if finalclass reverts.
            self.assertEqual(eng.FinalClassName, "Zoo")
            eng.rules()[-1].forget()
            self.assertEqual(eng.FinalClassName, "Animal")
        finally:
            box.flush_all()
    
    def test_zzz_Schema_Upgrade(self):
        # Must run last.
        zs = ZooSchema(root)
        
        # In this first upgrade, we simulate the case where the code was
        # upgraded, and the database schema upgrade performed afterward.
        # The Schema.latest property is set, and upgrade() is called with
        # no argument (which should upgrade us to "latest").
        Animal.set_property("ExhibitID")
        # Test numeric default (see hack in storeado for MS Access).
        prop = Animal.set_property("Stomachs", int)
        prop.default = 1
        zs.latest = 2
        zs.upgrade()
        
        # In this example, we simulate the developer who wants to put
        # model changes inline with database changes (see upgrade_to_3).
        # We do not set latest, but instead supply an arg to upgrade().
        zs.upgrade(3)
        
        # Test that Animals have a new "Family" property, and an ExhibitID.
        box = root.new_sandbox()
        try:
            emp = box.unit(Animal, Family='Emperor Penguin')
            self.assertEqual(emp.ExhibitID, 'The Penguin Encounter')
        finally:
            box.flush_all()


class KeyStoreTests(unittest.TestCase):
    """Timed tests for key/value storage styles common in HA environments."""
    
    numobjects = 500
    
    def test_01_Storage(self):
        today = datetime.date.today()
        
        start = datetime.datetime.now()
        for id in xrange(self.numobjects):
            root.reserve(Ticket(ID=id+1, Price=25.00, Date=today))
        if root.commit:
            root.commit()
        print ("%r objects stored in %s" %
               (self.numobjects, datetime.datetime.now() - start)),
    
    def test_02_Retrieval(self):
        start = datetime.datetime.now()
        for id in xrange(self.numobjects):
            ticket = root.unit(Ticket, ID=id+1)
            if ticket is None:
                self.fail("No such ticket %r" % id)
        print ("%r objects retrieved in %s" %
               (self.numobjects, datetime.datetime.now() - start)),


class ConcurrencyTests(unittest.TestCase):
    
    def test_Multithreading(self):
##        print "skipped ",
##        return
        
        # Test threads overlapping on separate sandboxes
        f = (lambda x: x.Legs == 4)
        def box_per_thread():
            # Notice that, although we write changes in each thread,
            # we only assert the unchanged data, since the order of
            # thread execution can not be guaranteed.
            box = root.new_sandbox()
            try:
                quadrupeds = box.recall(Animal, f)
                self.assertEqual(len(quadrupeds), 4)
                quadrupeds[0].Age += 1.0
            finally:
                box.flush_all()
        ts = []
        # PostgreSQL, for example, has a default max_connections of 100.
        for x in range(99):
            t = threading.Thread(target=box_per_thread)
            t.start()
            ts.append(t)
        for t in ts:
            t.join()
    
    def test_ContextManagement(self):
        # Test context management using Python 2.5 'with ... as'
        try:
            from dejavu.test import test_context
        except SyntaxError:
            print "'with ... as' not supported (skipped) ",
        else:
            test_context.test_with_context(root)


class NumericTests(unittest.TestCase):
    
    def test_long(self):
##        print "skipped ",
##        return
        
        box = root.new_sandbox()
        try:
            print "precision:",
            # PostgreSQL should be able to go up to 1000 decimal digits (~= 2 ** 10),
            # but SQL constants don't actually overflow until 2 ** 15. Meh.
            db = getattr(leaf_store(), "db", None)
            if db:
                maxprec = db.typeset.numeric_max_precision()
                if maxprec == 0:
                    # SQLite, for example, must always use TEXT.
                    # So we might as well try... oh... how about 3?
                    overflow_prec = 3
                else:
                    overflow_prec = int(math.log(maxprec, 2)) + 1
            else:
                overflow_prec = 8
            
            for prec in xrange(overflow_prec + 1):
                p = 2 ** prec
                print p,
                
                # We don't need to test <type long> at different 'scales'.
                root.drop_property(NothingToDoWithZoos, 'ALong')
                NothingToDoWithZoos.ALong.hints['bytes'] = p
                root.add_property(NothingToDoWithZoos, 'ALong')
                
                for neg in (False, True):
                    # Create an instance and set the specified precision
                    # Assume all numeric dbtypes are signed; test both
                    # positive and negative, and cut the min/max in half.
                    # Divide by 2 for signed type.
                    Lval = ((16 ** p) / 2) - 1
                    if neg:
                        Lval = 0 - Lval
                    box.memorize(NothingToDoWithZoos(ALong=Lval))
                    
                    # Flush and retrieve the object. Use comparisons to test
                    # decompilation of imperfect_type when using large numbers.
                    box.flush_all()
                    nothing = box.unit(NothingToDoWithZoos, ALong=Lval)
                    if nothing is None:
                        self.fail("Unit not found by long property. prec=%s" % p)
                    
                    # Test retrieved values.
                    if nothing.ALong != Lval:
                        self.fail("%r != %r prec=%s" % (nothing.ALong, Lval, p))
                    nothing.forget()
        finally:
            box.flush_all()
    
    def test_float(self):
##        print "skipped ",
##        return
        
        float_prec = 53
        box = root.new_sandbox()
        try:
            print "precision:",
            # PostgreSQL should be able to go up to 1000 decimal digits (~= 2 ** 10),
            # but SQL constants don't actually overflow until 2 ** 15. Meh.
            db = getattr(leaf_store(), "db", None)
            if db:
                maxprec = db.typeset.numeric_max_precision()
                if maxprec == 0:
                    # SQLite, for example, must always use TEXT.
                    # So we might as well try... oh... how about 3?
                    overflow_prec = 3
                else:
                    overflow_prec = int(math.log(maxprec, 2)) + 1
            else:
                overflow_prec = 8
            
            for prec in xrange(overflow_prec + 1):
                p = 2 ** prec
                print p,
                
                # Test scales at both extremes and the median
                for s in (0, int(prec/2), max(prec-1, 0)):
                    s = 2 ** s
                    
                    # Modify the model and storage
##                    if p <= float_prec:
                    root.drop_property(NothingToDoWithZoos, 'AFloat')
                    NothingToDoWithZoos.AFloat.hints['precision'] = p
                    root.add_property(NothingToDoWithZoos, 'AFloat')
                    
                    for neg in (False, True):
                        # Create an instance and set the specified precision
                        # and scale for all fields. Assume all numeric
                        # dbtypes are signed; test both positive and negative.
##                        if p <= float_prec:
                        fmax = (2 ** p) - 1
                        fval = float(fmax / (2 ** s))
                        if neg:
                            fval = 0 - fval
                        box.memorize(NothingToDoWithZoos(AFloat=fval))
                        
                        # Flush and retrieve the object. Use comparisons to test
                        # decompilation of imperfect_type when using large numbers.
##                        if p <= float_prec:
                        box.flush_all()
                        nothing = box.unit(NothingToDoWithZoos, AFloat=fval)
                        if nothing is None:
                            self.fail("Unit not found by float property. "
                                      "prec=%s scale=%s" % (p, s))
                        
                        # Test retrieved values.
##                        if p <= float_prec:
                        if nothing.AFloat != fval:
                            self.fail("%s != %s prec=%s scale=%s" %
                                      (`nothing.AFloat`, `fval`, p, s))
                        nothing.forget()
        finally:
            box.flush_all()
    
    def test_decimal_and_fixed(self):
        if not (decimal or fixedpoint):
            print "skipped (no decimal or fixedpoint libraries available)"
            return
        
        box = root.new_sandbox()
        try:
            print "precision:",
            # PostgreSQL should be able to go up to 1000 decimal digits (~= 2 ** 10),
            # but SQL constants don't actually overflow until 2 ** 15. Meh.
            db = getattr(leaf_store(), "db", None)
            if db:
                maxprec = db.typeset.numeric_max_precision()
                if maxprec == 0:
                    # SQLite, for example, must always use TEXT.
                    # So we might as well try... oh... how about 3?
                    overflow_prec = 3
                else:
                    overflow_prec = int(math.log(maxprec, 2)) + 1
            else:
                overflow_prec = 8
            
            dc = decimal.getcontext()
            
            for prec in xrange(overflow_prec + 1):
                p = 2 ** prec
                print p,
                if p > dc.prec:
                    dc.prec = p
                
                # Test scales at both extremes and the median
                for s in (0, int(prec/2), max(prec-1, 0)):
                    s = 2 ** s
                    
                    # Modify the model and storage
                    if decimal:
                        root.drop_property(NothingToDoWithZoos, 'ADecimal')
                        NothingToDoWithZoos.ADecimal.hints['precision'] = p
                        NothingToDoWithZoos.ADecimal.hints['scale'] = s
                        root.add_property(NothingToDoWithZoos, 'ADecimal')
                    if fixedpoint:
                        root.drop_property(NothingToDoWithZoos, 'AFixed')
                        NothingToDoWithZoos.AFixed.hints['precision'] = p
                        NothingToDoWithZoos.AFixed.hints['scale'] = s
                        root.add_property(NothingToDoWithZoos, 'AFixed')
                    
                    for neg in (False, True):
                        # Create an instance and set the specified precision
                        # and scale for all fields. Assume all numeric
                        # dbtypes are signed; test both positive and
                        # negative, and cut the min/max in half for long
                        # (floats and numerics don't need to be cut in half,
                        # because they have implicit sign bits).
                        nothing = NothingToDoWithZoos()
                        nval = "1" * p
                        nval = nval[:-s] + "." + nval[-s:]
                        if decimal:
                            dval = decimal.Decimal(nval)
                            if neg:
                                dval = 0 - dval
                            setattr(nothing, 'ADecimal', dval)
                        if fixedpoint:
                            # fixedpoint uses "precision" where we use "scale";
                            # that is, number of digits after the decimal point.
                            fpval = fixedpoint.FixedPoint(nval, s)
                            if neg:
                                fpval = 0 - fpval
                            setattr(nothing, 'AFixed', fpval)
                        box.memorize(nothing)
                        
                        # Flush and retrieve the object. Use comparisons to test
                        # decompilation of imperfect_type when using large numbers.
                        if decimal:
                            box.flush_all()
                            nothing = box.unit(NothingToDoWithZoos, ADecimal=dval)
                            if nothing is None:
                                self.fail("Unit not found by decimal property. "
                                          "prec=%s scale=%s" % (p, s))
                        if fixedpoint:
                            box.flush_all()
                            nothing = box.unit(NothingToDoWithZoos, AFixed=fpval)
                            if nothing is None:
                                self.fail("Unit not found by fixedpoint property. "
                                          "prec=%s scale=%s" % (p, s))
                        
                        # Test retrieved values.
                        if decimal:
                            if nothing.ADecimal != dval:
                                self.fail("%s != %s prec=%s scale=%s" %
                                          (`nothing.ADecimal`, `dval`, p, s))
                        if fixedpoint:
                            if nothing.AFixed != fpval:
                                self.fail("%s != %s prec=%s scale=%s" %
                                          (`nothing.AFixed`, `fpval`, p, s))
                        nothing.forget()
        finally:
            box.flush_all()


class DiscoveryTests(unittest.TestCase):
    
    def assertIn(self, first, second, msg=None):
        """Fail if 'second not in first'."""
        if not second.lower() in first.lower():
            raise self.failureException, (msg or '%r not in %r' % (second, first))
    
    def setUp(self):
        self.modeler = None
        
        s = leaf_store()
        if not hasattr(s, "db"):
            return
        
        # Clear out all mappings and re-discover
        dict.clear(s.schema)
        s.schema.discover_all()
        
        from dejavu.storage import db
        self.modeler = db.Modeler(s.schema)
    
    def test_make_classes(self):
        if not self.modeler:
            print "not a db (skipped) ",
            return
        
        for cls in (Zoo, Animal):
            tkey = self.modeler.schema.table_name(cls.__name__)
            
            uc = self.modeler.make_class(tkey, cls.__name__)
            self.assert_(not issubclass(uc, cls))
            self.assertEqual(uc.__name__, cls.__name__)
            
            # Both Zoo and Animal should have autoincrementing ID's
            # (but MySQL uses all lowercase identifiers).
            self.assertEqual(set([x.lower() for x in uc.identifiers]),
                             set([x.lower() for x in cls.identifiers]))
            self.assert_(isinstance(uc.sequencer, UnitSequencerInteger),
                         "%r sequencer is of type %r (expected %r)"
                         % (cls, type(uc.sequencer), UnitSequencerInteger))
            
            for pname in cls.properties:
                cname = self.modeler.schema.column_name(tkey, pname)
                copy = getattr(uc, cname)
                orig = getattr(cls, pname)
                self.assertEqual(copy.key, cname)
                # self.assertEqual(copy.type, orig.type)
                self.assertEqual(copy.default, orig.default,
                                 "%s.%s default %s != copy %s"
                                 % (cls.__name__, pname,
                                    `orig.default`, `copy.default`))
                
                for k, v in orig.hints.iteritems():
                    if isinstance(v, (int, long)):
                        v2 = copy.hints.get(k)
                        if v2 != 0 and v2 < v:
                            self.fail("%s.%s hints[%s]: %s not >= %s" %
                                      (cls.__name__, pname, k, v2, v))
                    else:
                        self.assertEqual(copy.hints[k], v)
    
    def test_make_source(self):
        if not self.modeler:
            print "not a db (skipped) ",
            return
        
        tkey = self.modeler.schema.table_name('Exhibit')
        source = self.modeler.make_source(tkey, 'Exhibit')
        
        classline = "class Exhibit(Unit):"
        if not source.lower().startswith(classline.lower()):
            self.fail("%r does not start with %r" % (source, classline))
        
        clsname = self.modeler.schema.__class__.__name__
        if "SQLite" in clsname:
            # SQLite's internal types are teh suck.
            self.assertIn(source, "    Name = UnitProperty(")
            self.assertIn(source, "    ZooID = UnitProperty(")
            self.assertIn(source, "    PettingAllowed = UnitProperty(")
            self.assertIn(source, "    Acreage = UnitProperty(")
            self.assertIn(source, "    sequencer = UnitSequencer")
        else:
            try:
                self.assertIn(source, "    Name = UnitProperty(unicode")
            except AssertionError:
                self.assertIn(source, "    Name = UnitProperty(str")
            
            self.assertIn(source, "    ZooID = UnitProperty(int")
            if "Firebird" in clsname:
                # Firebird doesn't have a bool datatype
                self.assertIn(source, "    PettingAllowed = UnitProperty(int")
            else:
                self.assertIn(source, "    PettingAllowed = UnitProperty(bool")
            if decimal:
                self.assertIn(source, "    Acreage = UnitProperty(decimal.Decimal")
            else:
                self.assertIn(source, "    Acreage = UnitProperty(float")
            
            self.assertIn(source, "    sequencer = UnitSequencer()")
        
        
        if "    ID = UnitProperty" in source:
            self.fail("Exhibit incorrectly possesses an ID property.")
        
        # ID = None should remove the existing ID property
        self.assertIn(source, "    ID = None")
        
        for items in ["'zooid', 'name'", "'name', 'zooid'",
                      "u'zooid', u'name'", "u'name', u'zooid'"]:
            if ("    identifiers = (%s)" % items) in source.lower():
                break
        else:
            self.fail("%r not found in %r" %
                      ("    identifiers = ('ZooID', 'Name')", source))


root = None

def leaf_store():
    try:
        child = root.stores.values()[0]
    except AttributeError:
        # Not a mediated store
        child = root
    return child


class ZooSchema(dejavu.Schema):
    
    # We set "latest" to 1 so we can test upgrading manually.
    latest = 1
    
    def upgrade_to_2(self):
        self.store.add_property(Animal, "Stomachs")
        self.store.add_property(Animal, "ExhibitID")
        box = self.store.new_sandbox()
        for exhibit in box.recall(Exhibit):
            for animalID in exhibit.Animals:
                # Use the Sandbox magic recaller method.
                a = box.Animal(animalID)
                if a:
                    # Exhibits are identified by ZooID and Name
                    a.ZooID = exhibit.ZooID
                    a.ExhibitID = exhibit.Name
        box.flush_all()
    
    def upgrade_to_3(self):
        Animal.remove_property("Species")
        Animal.set_property("Family")
        
        # Note that we drop this column in a separate step from step 2.
        # If we had mixed model properties and SM properties in step 2,
        # we could have done this all in one step. But this is a better
        # demonstration of the possibilities. ;)
        Exhibit.remove_property("Animals")
        self.store.drop_property(Exhibit, "Animals")
        
        self.store.rename_property(Animal, "Species", "Family")


def setup(store, mediated=False):
    """Set up storage for Zoo classes."""
    
    global root
    store.register_all(globals())
    engines.register_classes(store)
    dejavu.DeployedVersion.register(store)
    if hasattr(store, "cache"):
        store.cache.register_all(globals())
        engines.register_classes(store.cache)
        dejavu.DeployedVersion.register(store.cache)
    if hasattr(store, "nextstore"):
        store.nextstore.register_all(globals())
        engines.register_classes(store.nextstore)
        dejavu.DeployedVersion.register(store.nextstore)
    
    if mediated:
        from dejavu.storage import partitions
        root = partitions.VerticalPartitioner()
        root.add_store('testSM', store)
    else:
        root = store
    
    if not root.has_database():
        print "Creating database..."
        root.create_database()
    assert root.has_database() == True
    zs = ZooSchema(root)
    zs.upgrade()
    zs.assert_storage()


def teardown():
    """Tear down storage for Zoo classes."""
    # Manually drop each table just to test that code.
    # Call map_all first in case our discovery tests screwed up the keys.
    root.map_all(conflicts='ignore')
    
    for cls in root.classes:
        try:
            root.drop_storage(cls, conflicts='warn')
        except KeyError:
            pass
    
    root.drop_database(conflicts='warn')
    root.shutdown(conflicts='warn')

def run(store, mediated=False):
    """Run the zoo fixture."""
    try:
        try:
            setup(store, mediated)
            loader = unittest.TestLoader().loadTestsFromTestCase
            
            # Run the ZooTests and time it.
            zoocase = loader(ZooTests)
            startTime = datetime.datetime.now()
            tools.djvTestRunner.run(zoocase)
            print "Ran zoo cases in:", datetime.datetime.now() - startTime
            
            # Run the other cases.
            tools.djvTestRunner.run(loader(KeyStoreTests))
            tools.djvTestRunner.run(loader(NumericTests))
            
            # Each thread opens a new SQLite :memory: database,
            # so the concept of "concurrency" is pretty meaningless.
            db = getattr(leaf_store(), "db", None)
            if db is None or db.name != ':memory:':
                tools.djvTestRunner.run(loader(ConcurrencyTests))
            
            tools.djvTestRunner.run(loader(DiscoveryTests))
        except:
            traceback.print_exc()
    finally:
        teardown()
