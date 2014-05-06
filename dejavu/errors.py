"""Exception classes for Dejavu."""


class DejavuError(Exception):
    """Base class for errors which occur within Dejavu."""
    def __init__(self, *args):
        Exception.__init__(self)
        self.args = args
    
    def __str__(self):
        return u'\n'.join([unicode(arg) for arg in self.args])

class AssociationError(DejavuError):
    """Exception raised when a Unit association fails."""
    pass

class UnrecallableError(DejavuError):
    """Exception raised when a Unit was sought but not recalled."""
    pass

class StorageWarning(UserWarning):
    """Warning about functionality which is not supported by all SM's."""
    pass

class MappingError(DejavuError):
    """Exception raised when a Unit class cannot be mapped to storage.
    
    This exception should be raised when a consumer attempts to build
    a map between a Unit class and existing internal storage structures.
    Other exceptions may be raised when trying to find such a map after
    it has already (supposedly) been created. That is, the questions
    "do we have a map?" and "can we create a map?" are distinct.
    The latter should raise this exception whenever possible.
    The behavior of the former is not specified.
    """
    pass



# -------------------------- Conflict handling -------------------------- #


class ConflictHandler(object):
    """Dispatcher for behavior upon encountering a mapping conflict.
    
    When MappingErrors are encountered, a singleton instance of this class
    is used to respond to the error based upon a supplied mode. You can add
    failure modes (and their corresponding behaviors) by adding methods to
    this class.
    
    In general, only leaf nodes need call this object; pure proxies and
    storage mixers which simply pass calls through to (possibly multiple)
    child storage nodes may safely rely on the behavior of their child
    nodes by passing the 'conflicts' argument through to all children.
    """
    
    def __call__(self, mode, msg):
        """React to a conflict according to the given mode.
        
        mode: This argument determines what happens when there are
        discrepancies between the Dejavu model and the actual database.
        
            If 'error' (the default), MappingError is raised for the
            first issue and the call is aborted.
            
            If 'warn', then a StorageWarning is raised (instead of an error)
            for each issue, and the call is not aborted. This allows you to
            see all errors at once, without having to stop and fix each one
            and then execute the call again.
            
            If 'repair', then each issue will be resolved by changing
            the database to match the model. Not all calls support this
            mode for all errors; any which do not support this mode will
            error instead.
            
            If 'ignore', any model conflicts are silently ignored.
            Use of this mode causes mandelbugs. You have been warned.
        """
        func = getattr(self, mode, None)
        if func is None:
            raise ValueError("Conflict mode %r not recognized." % mode)
        return func(msg)
    
    def warn(self, msg):
        import warnings
        warnings.warn(msg, StorageWarning, stacklevel=3)
    
    def ignore(self, msg):
        pass
    
    def error(self, msg):
        raise MappingError(msg)

conflict = ConflictHandler()

