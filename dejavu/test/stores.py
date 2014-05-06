import datetime
import getpass
import new
import os
localDir = os.path.join(os.getcwd(), os.path.dirname(__file__))


def run(storename, olap=False, mediated=False):
    storefunc = globals()[storename]
    if olap:
        import clinic_fixture
        reload(clinic_fixture)
        print "OLAP Tests for %r" % storename
        storefunc(clinic_fixture, mediated)
    else:
        import zoo_fixture
        reload(zoo_fixture)
        print "OLTP Tests for %r" % storename
        storefunc(zoo_fixture, mediated)


logname = os.path.join(localDir, "djvtest.log")

def _djvlog(self, message):
    """Dejavu logger (writes to error.log)."""
    if isinstance(message, unicode):
        message = message.encode('utf8')
    sm = self.__class__.__name__.replace("StorageManager", "")
    s = "%s %s %s" % (datetime.datetime.now().isoformat(), sm, message)
    f = open(logname, 'ab')
    f.write(s + '\n')
    f.close()

def hook_into_log(sm):
    sm.log = new.instancemethod(_djvlog, sm, sm.__class__)
    
    import dejavu
    sm.logflags = (dejavu.logflags.ERROR + dejavu.logflags.IO +
                   dejavu.logflags.SQL + dejavu.logflags.STORE)


def get_store(sm, opts):
    
    opts['Prefix'] = 'test'
    
    from dejavu import storage
    root = storage.resolve(sm, opts)
    hook_into_log(root)
    
    v = getattr(root, "version", None)
    if v:
        print v()
    
    return root


# --------------------------- Individual Stores --------------------------- #


def caching(fixture, mediated):
    from dejavu import storage
    nextstore = storage.resolve('shelve', {u'Path': localDir})
    hook_into_log(nextstore)
    sm = get_store("cache", {'Next Store': nextstore})
    hook_into_log(sm.cache)
    fixture.run(sm, mediated)


def aged(fixture, mediated):
    from dejavu import storage
    nextstore = storage.resolve('shelve', {u'Path': localDir})
    hook_into_log(nextstore)
    
    opts = {'Lifetime': '10 minutes',
            'Next Store': nextstore,
            }
    sm = get_store("aged", opts)
    hook_into_log(sm.cache)
    fixture.run(sm, mediated)


def burned(fixture, mediated):
    from dejavu import storage
    nextstore = storage.resolve('shelve', {u'Path': localDir})
    hook_into_log(nextstore)
    sm = get_store("burned", {'Next Store': nextstore})
    hook_into_log(sm.cache)
    fixture.run(sm, mediated)


def firebird(fixture, mediated):
    try:
        import kinterbasdb
    except ImportError:
        print ("The kinterbasdb module could not be imported. "
               "The Firebird test will not be run.")
        return
    
    user = "sysdba"
    passwd = getpass.getpass("Enter the password for the Firebird '%s' user: " % user)
    
    # Note that "the Firebird 1.5 client library on Windows is thread-safe
    # if the remote protocol is used ... but is not thread-safe if the
    # local protocol is used, ..."
    opts = {'host': "localhost",
            'name': os.path.join(localDir, "testdb", r"test.fdb"),
            'user': user,
            'password': passwd,
            }
    fixture.run(get_store("firebird", opts), mediated)


def fs(fixture, mediated):
    opts = {'root': os.path.join(localDir, "testdb", "fsroot"),
            'Exhibit.Name': '.mp3',
            }
    fixture.run(get_store("folders", opts), mediated)


def json(fixture, mediated):
    opts = {'root': os.path.join(localDir, "testdb", "jsonroot"),
            }
    fixture.run(get_store("json", opts), mediated)


def memcached(fixture, mediated):
    opts = {'memcached.servers': ['127.0.0.1:11211'],
            'memcached.indexed': True,
            'name': 'djvtest',
            }
    fixture.run(get_store("memcached", opts), mediated)

def memcached2(fixture, mediated):
    opts = {'memcached.servers': ['127.0.0.1:11211'],
            'memcached.indexed': False,
            'name': 'djvtest',
            }
    mc = get_store("memcached", opts)
    
    nextstore = get_store('shelve', {u'Path': localDir})
    
    sm = get_store("cache", {'Next Store': nextstore, 'cache': mc})
    fixture.run(sm, mediated)


def msaccess(fixture, mediated):
    try:
        import pythoncom
    except ImportError:
        print ("The pythoncom module could not be imported. "
               "The MSAccess test will not be run.")
        return
    
    from geniusql.providers import ado
    try:
        ado.gen_py()
    except ImportError:
        print ("ADO 2.7 support could not be verified. "
               "The MSAccess test will not be run.")
        return
    
    opts = {'connections.Connect':
            "PROVIDER=MICROSOFT.JET.OLEDB.4.0;DATA SOURCE=%s;" %
            os.path.join(localDir, "zoo.mdb"),
            }
    fixture.run(get_store("access", opts), mediated)


def mysql(fixture, mediated):
    try:
        import _mysql
    except ImportError:
        print("The _mysql module could not be imported. "
              "The test for MySQL will not be run.")
        return
    
    opts = {"host": "localhost",
            "db": "dejavu_test",
            "user": "root",
            }
    opts['passwd'] = getpass.getpass("Enter the password for the MySQL '%s' user:"
                               % opts['user'])
    
    for encoding in ['latin1', 'utf8']:
        reload(fixture)
        opts['encoding'] = encoding
        print "\nTesting the %r encoding" % encoding
        fixture.run(get_store("mysql", opts), mediated)


def proxy(fixture, mediated):
    from dejavu import storage
    nextstore = storage.resolve('shelve', {u'Path': localDir})
    hook_into_log(nextstore)
    
    opts = {'Lifetime': '10 minutes',
            'Next Store': nextstore,
            }
    fixture.run(get_store("proxy", opts), mediated)


def psycopg(fixture, mediated):
    try:
        try:
            # If possible, you should copy the _psycopg.pyd file into a top level
            # so the SM can avoid importing the entire package.
            import _psycopg
        except ImportError:
            from psycopg2 import _psycopg
    except ImportError:
        print("The psycopg2._psycopg module could not be imported. "
              "The psycopg test will not be run.")
        return
    
    user = "postgres"
    passwd = getpass.getpass("Enter the password for the PostgreSQL '%s' user:" % user)
    
    opts = {'connections.Connect':
            ("host=localhost dbname=dejavu_test user=%s password=%s"
             % (user, passwd)),
            }
    
    for encoding in ['SQL_ASCII', 'UNICODE']:
        reload(fixture)
        opts['encoding'] = encoding
        print "\nTesting the %r encoding" % encoding
        fixture.run(get_store("psycopg", opts), mediated)


def pypgsql(fixture, mediated):
    try:
        from pyPgSQL import libpq
    except ImportError:
        print("The pyPgSQL.libpq module could not be imported. "
              "The pyPgSQL test will not be run.")
        return
    
    user = "postgres"
    passwd = getpass.getpass("Enter the password for the PostgreSQL '%s' user:" % user)
    opts = {'connections.Connect':
            ("host=localhost dbname=dejavu_test user=%s password=%s"
             % (user, passwd)),
            }
    
    for encoding in ['SQL_ASCII', 'UNICODE']:
        reload(fixture)
        opts['encoding'] = encoding
        print "\nTesting the %r encoding" % encoding
        fixture.run(get_store("postgres", opts), mediated)


def ram(fixture, mediated):
    fixture.run(get_store("ram", {}), mediated)


def shelve(fixture, mediated):
    """Test the shelve Storage Manager for dejavu.
    
    Notice that, since StorageManagerShelve doesn't decompile any Expressions,
    this will also test all native dejavu logic functions and any other aspects
    of Expression(unit).
    """
    opts = {u'Path': os.path.join(localDir, "testdb")}
    fixture.run(get_store("shelve", opts), mediated)


def sqlite(fixture, mediated):
    _sqlite = None
    try:
        # Use _sqlite3 directly to avoid all of the DB-API overhead.
        # This assumes the one built into Python 2.5+
        import _sqlite3 as _sqlite
    except ImportError:
        # Use _sqlite directly to avoid all of the DB-API overhead.
        # This will import the "old API for SQLite 3.x",
        # using e.g. pysqlite 1.1.7
        try:
            # Is the single module on the python path?
            import _sqlite
        except ImportError:
            # Try pysqlite2
            try:
                from pysqlite2 import _sqlite
            except ImportError:
                print("The _sqlite module could not be imported. "
                      "The SQLite test will not be run.")
                return
    
    print "\nTesting :memory: database"
    fixture.run(get_store("sqlite", {'Database': ':memory:'}), mediated)
    
    print "\nTesting file database"
    reload(fixture)
    opts = {"Database": os.path.join(localDir, "testdb", "sqlite_zoo_test")}
    fixture.run(get_store("sqlite", opts), mediated)


def sqlserver(fixture, mediated):
    try:
        import pythoncom
    except ImportError:
        print ("The pythoncom module could not be imported. "
               "The SQLServer test will not be run.")
        return
    
    from geniusql.providers import ado
    try:
        ado.gen_py()
    except ImportError:
        print ("ADO 2.7 support could not be verified. "
               "The SQLServer test will not be run.")
        return
    
    opts = {'connections.Connect':
            # SQL Server 2000
##            ("Provider=SQLOLEDB.1; Integrated Security=SSPI; "
##             "Initial Catalog=dejavu_test; Data Source=(local)"),
            # SQL Server 2005
            ("Provider=SQLNCLI; Integrated Security=SSPI; "
             "Initial Catalog=dejavu_test; Data Source=(local)\VAIO_VEDB"),
            # Shorten the transaction deadlock timeout.
            # You may need to adjust this for your system.
            'connections.CommandTimeout': 10,
            }
    fixture.run(get_store("sqlserver", opts), mediated)

