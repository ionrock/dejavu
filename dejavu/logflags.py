"""Log flags (and their associated messages) for Dejavu."""


class LogCategory(int):
    
    def __new__(cls, value, message=None):
        return int.__new__(cls, value)
    
    def __init__(self, value, message=None):
        if message is not None:
            self.message = message
    
    def message(self, *args):
        return " ".join(map(repr, args))


ERROR = LogCategory(1)
def message(*args):
    return "IO: " + " ".join(map(repr, args))
IO = LogCategory(2, message)
SQL = LogCategory(4)

def message(msg):
    return "DDL: %s" % msg
DDL = LogCategory(8, message)

def message(store, cls):
    return "REGISTER: %r in store %r" % (cls, store)
REGISTER = LogCategory(16, message)



def message(unit):
    return "RESERVE %s: %s" % (unit.__class__.__name__,
                               repr(unit.identity())[:100])
RESERVE = LogCategory(128, message)

def message(classes, expr):
    import dejavu
    if isinstance(classes, dejavu.UnitJoin):
        target = repr(classes)
    else:
        target = classes.__name__
    return "RECALL %s: %r" % (target, expr)
RECALL = LogCategory(256, message)

def message(query, distinct):
    d = ""
    if distinct:
        d = " (distinct)"
    return "VIEW: %s%s" % (query, d)
VIEW = LogCategory(512, message)

def message(unit, forceSave):
    fs = ""
    if forceSave:
        fs = " (forced)"
    return "SAVE %s: %s%s" % (unit.__class__.__name__,
                              repr(unit.identity())[:100], fs)
SAVE = LogCategory(1024, message)

def message(unit):
    return "DESTROY %s: %s" % (unit.__class__.__name__,
                               repr(unit.identity())[:100])
DESTROY = LogCategory(2048, message)

STORE = LogCategory(DDL | RESERVE | RECALL | VIEW | SAVE | DESTROY)

del message
