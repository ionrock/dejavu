"""A harness for running a suite of Dejavu tests all at once.

Should be invoked as a script.
"""

import getopt
import os
localDir = os.path.dirname(__file__)
import sys
import unittest


class djvTestHarness(object):
    """A test harness for Dejavu."""
    
    cover = False
    olap = False
    mediated = False
    conquer = False
    
    def __init__(self, available_tests):
        """Constructor to populate the TestHarness instance.
        
        available_tests should be a list of module or store names.
        """
        self.available_tests = available_tests
        self.tests = []
    
    def load(self, args=sys.argv[1:]):
        """Populate a TestHarness from sys.argv.
        
        args defaults to sys.argv[1:], but you can provide a different
            set of args if you like.
        """
        
        longopts = ['help', 'cover', 'olap', 'mediated', 'conquer']
        longopts.extend(self.available_tests)
        try:
            opts, args = getopt.getopt(args, "", longopts)
        except getopt.GetoptError:
            # print help information and exit
            self.help()
            sys.exit(2)
        
        self.tests = []
        
        for o, a in opts:
            if o == '--help':
                self.help()
                sys.exit()
            elif o == '--cover':
                self.cover = True
            elif o == '--olap':
                self.olap = True
            elif o == '--mediated':
                self.mediated = True
            elif o == '--conquer':
                self.conquer = True
            else:
                o = o[2:]
                if o and o not in self.tests:
                    self.tests.append(o)
        
        if not self.tests:
            self.tests = self.available_tests[:]
    
    def help(self):
        """Print help for test.py command-line options."""
        
        print """
Dejavu Test Program
    Usage:
        test.py --help --olap --cover --mediated --conquer --<testname>
        
        --help: This help screen
        --olap: Run the OLAP fixture instead of the default OLTP fixture
        --cover: Run coverage tools
        --mediated: Use a VerticalPartitioner to mediate the store(s).
        --conquer: use pyconquer to trace calls.
        
        tests:"""
        for name in self.available_tests:
            print '        --' + name
    
    def run(self, conf=None):
        """Run the test harness."""
        self.load()
        
        if self.cover:
            self.start_coverage()
            try:
                self._run()
            finally:
                self.stop_coverage()
        elif self.conquer:
            import pyconquer
            f = os.path.join(os.path.dirname(__file__), "conquer.log")
            tr = pyconquer.Logger("dejavu", events=pyconquer.all_events)
            tr.out = open(f, "wb")
            try:
                tr.start()
                self._run()
            finally:
                tr.stop()
                tr.out.close()
        else:
            self._run()
    
    def _run(self):
        # Delay the import of any dejavu module so coverage can work
        # on the import (including class and func defs) as well.
        from dejavu.test import tools
        tools.prefer_parent_path()
        
        import dejavu
        print "Python version:", sys.version.split()[0]
        print "Dejavu version:", dejavu.__version__
        
        for name in self.tests:
            print
            if name.startswith("test_"):
                print "Running test module %r" % name
                suite = unittest.TestLoader().loadTestsFromName(name)
                from dejavu.test import tools
                tools.djvTestRunner.run(suite)
            else:
                from dejavu.test import stores
                stores.run(name, self.olap, self.mediated)
    
    def start_coverage(self):
        """Start the coverage tool.
        
        To use this feature, you need to download 'coverage.py',
        either Gareth Rees' original implementation:
        http://www.garethrees.org/2001/12/04/python-coverage/
        
        or Ned Batchelder's enhanced version:
        http://www.nedbatchelder.com/code/modules/coverage.html
        
        If neither module is found in PYTHONPATH,
        coverage is silently(!) disabled.
        """
        try:
            from coverage import the_coverage as coverage
            c = os.path.join(localDir, "coverage.cache")
            coverage.cache_default = c
            if c and os.path.exists(c):
                os.remove(c)
            coverage.start()
        except ImportError:
            coverage = None
        self.coverage = coverage
    
    def stop_coverage(self):
        """Stop the coverage tool, save results, and report."""
        if self.coverage:
            self.coverage.save()
            self.report_coverage()
    
    def report_coverage(self):
        """Print a summary from the code coverage tool."""
        
        # Assume we want to cover everything in "../../dejavu/"
        basedir = os.path.normpath(os.path.join(os.getcwd(), localDir, '../'))
        basedir = basedir.lower()
        self.coverage.get_ready()
        
        morfs = []
        for x in self.coverage.cexecuted:
            if x.lower().startswith(basedir):
                morfs.append(x)
        
        total_statements = 0
        total_executed = 0
        
        print
        print "CODE COVERAGE (this might take a while)",
        for morf in morfs:
            sys.stdout.write(".")
            sys.stdout.flush()
##            name = os.path.split(morf)[1]
            if morf.find('test') != -1:
                continue
            try:
                _, statements, _, missing, readable  = self.coverage.analysis2(morf)
                n = len(statements)
                m = n - len(missing)
                total_statements = total_statements + n
                total_executed = total_executed + m
            except KeyboardInterrupt:
                raise
            except:
                # No, really! We truly want to ignore any other errors.
                pass
        
        pc = 100.0
        if total_statements > 0:
            pc = 100.0 * total_executed / total_statements
        
        print ("\nTotal: %s Covered: %s Percent: %2d%%"
               % (total_statements, total_executed, pc))


def run():
    
    avail = ['test_analysis',
             'test_containers',
             'test_dejavu',
             
             'ram',
             'shelve',
             'proxy',
             'caching',
             'aged',
             'burned',
             'fs',
            
             'firebird',
             'json',
             'memcached',
             'memcached2',
             'msaccess',
             'mysql',
             'psycopg',
             'pypgsql',
             'sqlite',
             'sqlserver',
             ]
    djvTestHarness(avail).run()


if __name__ == '__main__':
    run()
