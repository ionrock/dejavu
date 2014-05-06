"""JSON conversion support for Dejavu Units."""

import datetime
import decimal
import time

import dejavu
from simplejson import JSONEncoder, JSONDecoder

__all__ = ["Encoder", "Decoder", "Converter"]


class Encoder(JSONEncoder):
    """Extends the base simplejson JSONEncoder for date and decimal types."""
    
    def push_date(self, d):
        """Serialize the given datetime.date object to a JSON string."""
        # Default is ISO 8601 compatible (standard notation).
        return "%04d-%02d-%02d" % (d.year, d.month, d.day)
    
    def push_time(self, t):
        """Serialize the given datetime.time object to a JSON string."""
        # Default is ISO 8601 compatible (standard notation).
        return "%02d:%02d:%02d" % (t.hour, t.minute, t.second)
    
    def push_datetime(self, dt):
        """Serialize the given datetime.datetime object to a JSON string."""
        # Default is ISO 8601 compatible (standard notation).
        # Don't use strftime because that can't handle dates before 1900.
        return ("%04d-%02d-%02d %02d:%02d:%02d" %
                (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second))
    
    def default(self, o):
        # We MUST check for a datetime.datetime instance before datetime.date.
        # datetime.datetime is a subclass of datetime.date, and therefore
        # instances of it are also instances of datetime.date.
        if isinstance(o, datetime.datetime):
            return {'__datetime__': True, 'value': self.push_datetime(o)}
        elif isinstance(o, datetime.date):
            return {'__date__': True, 'value': self.push_date(o)}
        elif isinstance(o, datetime.time):
            return {'__time__': True, 'value': self.push_time(o)}
        elif isinstance(o, decimal.Decimal):
            return {'__decimal__': True, 'value': unicode(o)}
        else:
            return JSONEncoder.default(self, o)


class Decoder(JSONDecoder):
    """Extends the base simplejson JSONDecoder for Dejavu."""
    
    def __init__(self, encoding=None, object_hook=None):
        if object_hook is None:
            object_hook = self.json_to_python
        JSONDecoder.__init__(self, encoding, object_hook)
    
    def pull_date(self, value):
        """Return a datetime.date object from the given JSON string."""
        chunks = (value[0:4], value[5:7], value[8:10])
        return datetime.date(*map(int, chunks))
    
    def pull_time(self, value):
        """Return a datetime.time object from the given JSON string."""
        chunks = (value[0:2], value[3:5], value[6:8])
        return datetime.time(*map(int, chunks))
    
    def pull_datetime(self, value):
        """Return a datetime.datetime object from the given JSON string."""
        chunks = (value[0:4], value[5:7], value[8:10],
                  value[11:13], value[14:16], value[17:19],
                  value[20:26] or 0)
        return datetime.datetime(*map(int, chunks))
    
    def json_to_python(self, d):
        if '__datetime__' in d:
            return self.pull_datetime(d['value'])
        if '__date__' in d:
            return self.pull_date(d['value'])
        if '__time__' in d:
            return self.pull_time(d['value'])
        if '__decimal__' in d:
            return decimal.Decimal(d['value'])
        return d


class Converter(object):
    """Provides two-way conversion of Units/JSON via loads and dumps methods.
    
    Also converts datetime.date, datetime.time, datetime.datetime and
    decimal.Decimal to/from JSON.

    This is accomplished by the Encoder and Decoder classes, which are
    subclasses of their counterparts in simplejson.  If you wish to change
    the output of the converter at all, you should probably subclass the
    Encoder/Decoder and then make a cusom Converter subclass with your
    encoder/decoder as class attributes.
    """
    
    encoder = Encoder
    decoder = Decoder
    
    def loads(self, s, encoding=None, **kw):
        return self.decoder(encoding=encoding, **kw).decode(s)

    def dumps(self, obj, skipkeys=False, ensure_ascii=False,
              check_circular=True, allow_nan=True, indent=None, **kw):
        return self.encoder(skipkeys, ensure_ascii, check_circular,
                            allow_nan, indent, **kw).encode(obj)


def unit_to_dict(unit):
    """Return a JSON-able dict for the given Dejavu Unit instance."""
    d = unit._properties.copy()
    d['__dejavu.class__'] = unit.__class__.__name__
    return d


def dict_to_unit(d, store=None, cls=None):
    """Return a Unit instance from a dict of UnitProperty values."""
    if cls is None:
        clsname = d['__dejavu.class__']
        cls = store.class_by_name(clsname)
    
    unit = cls()
    # Set _properties directly to avoid __set__ overhead.
    # As always, this must be FULLY populated, even if some values are None.
    params = {}
    for k in cls.properties:
        v = d.get(k, None)
        # The JSON serializer stores tuples as lists
        if v is not None and getattr(cls, k).type is tuple:
            v = tuple(v)
        params[k] = v
    unit._properties = params
    unit.cleanse()
    return unit
