#!/usr/bin/env python
#
# pgsql.py version 0.9.7
#
# Updated by Cristian Gafton to use the bind parameters
# and adapt to the new interface provided by the C bindings
#
# Originally written by D'Arcy J.M. Cain
#

"""pgsql - DB-API 2.0 compliant module for PostgreSQL.

<c> 2009 Sune Kirkeby <mig@ibofobi.dk>, CSIS A/S.

(c) 2006-2007 Cristian Gafton <gafton@rpath.com>, rPath, Inc.
Reworked the C and Python modules based on PyGreSQL sources

(c) 1999, Pascal Andre <andre@via.ecp.fr>.
See COPYRIGHT file for further information on copyright.

Inline documentation is sparse.
See DB-API 2.0 specification for usage information:
http://www.python.org/peps/pep-0249.html

See the README file for an overview of this module's contents.
"""

import re
import warnings
from math import floor, modf
from time import localtime, strptime
from decimal import Decimal

import _pgsql
from _pgsql import TRANS_ACTIVE, TRANS_IDLE, \
                   TRANS_INERROR, TRANS_INTRANS, TRANS_UNKNOWN
from _pgsql import InterfaceError, DatabaseError, InternalError, \
     OperationalError, ProgrammingError, IntegrityError, DataError, \
     NotSupportedError, Error, Warning

from datetime import datetime, date, time, timedelta

# Work-around time.strptime not being thread-safe (see
# <http://bugs.python.org/issue7980>) by making sure _strptime is imported.
import _strptime

# compliant with DB SIG 2.0
apilevel = '2.0'

# module may be shared, but not connections
threadsafety = 1

# this module use ANSI C-style printf format codes
paramstyle = 'format'

# PostgreSQL type OIDs
INET_TYPE_OID = 869
CIDR_TYPE_OID = 650
BYTEA_TYPE_OID = 17

class pg_typed_value(object):
    '''Special wrapper for postgresql values for which we want to keep their
    type's OID, so we can safely round-trip them through Python. Also acts as
    a typecaster.'''

    __slots__ = ['value', '__pgsql_typeoid__']

    def __init__(self, typoid, value):
        if isinstance(value, buffer):
            value = str(value)

        assert isinstance(typoid, int), `typoid`
        assert isinstance(value, str), `value`

        self.__pgsql_typeoid__ = typoid
        self.value = value

    def __str__(self):
        return self.value

    def __repr__(self):
        return '<%s %d %r>' % (self.__class__.__name__,
                               self.__pgsql_typeoid__,
                               self.value)

    def __cmp__(self, other):
        return cmp(self.value, other)

# convert to Python types the values that were not automatically
# converted by pgsql.c
def typecast_date(typ, value):
    t = strptime(value, '%Y-%m-%d')
    return Date(*t[:3])

def typecast_datetime(typ, value):
    date, time = value.split(' ', 1)
    time = typecast_time(typ, time)
    date = strptime(date, '%Y-%m-%d')
    return datetime(date[0], date[1], date[2],
                    time.hour, time.minute, time.second, time.microsecond)

def typecast_time(typ, value):
    if '+' in value:
        if value.endswith('+00'):
            value = value[:-3]
        else:
            raise NotImplementedError('Can not represent timezones in Python')

    if '.' in value:
        value, micros = value.split('.')
        micros = micros + ('0' * (6 - len(micros)))
        micros = int(micros)
    else:
        micros = 0

    t = strptime(value, '%H:%M:%S')
    return time(t[3], t[4], t[5], micros)

class interval(object):
    '''I am a PostgreSQL interval.'''
    __slots__ = ['years', 'months', 'days', 'hours', 'minutes', 'seconds',
                 'microseconds']

    def __init__(self, years=0, months=0, days=0,
                       hours=0, minutes=0, seconds=0, microseconds=0):
        values = locals()
        for attr in self.__slots__:
            setattr(self, attr, values[attr])

    def to_timedelta(self):
        '''Convert to a datetime.timedelta instance.

        This can fail if I I represent an interval measured in years or
        months, since these have no fixed length. This error results in
        a ValueError exception.'''
        if self.years or self.months:
            raise ValueError('Interval with years or months cannot be '
                             'converted to timedelta')
        seconds = self.hours * 60 * 60 + self.minutes * 60 + self.seconds
        return timedelta(self.days, seconds, self.microseconds)

    def __eq__(self, other):
        if not isinstance(other, interval):
            return NotImplemented
        for attr in self.__slots__:
            if getattr(self, attr) <> getattr(other, attr):
                return False
        return True
    def __ne__(self, other):
        return not self == other

    def __str__(self):
        pieces = []
        for attr in self.__slots__:
            value = getattr(self, attr)
            if value:
                pieces.append('%d %s' % (value, attr))
                if abs(value) == 1:
                    pieces[-1] = pieces[-1][:-1]
        if pieces:
            return ' '.join(pieces)
        else:
            return '0 seconds'

    def __repr__(self):
        pieces = []
        for attr in self.__slots__:
            value = getattr(self, attr)
            if value:
                pieces.append('%s=%d' % (attr, value))
        return '%s(%s)' % (self.__class__.__name__, ', '.join(pieces))

denominators = {
    'day': 'days',
    'year': 'years',
    'mon': 'months', 'mons': 'months'
}
def typecast_interval(typ, value):
    result = {}

    pieces = value.split()
    while len(pieces) > 1:
        value, denominator, pieces = pieces[0], pieces[1], pieces[2:]
        attribute = denominators.get(denominator, denominator)
        result[attribute] = int(value)

    if pieces:
        t = typecast_time(typ, pieces.pop())
        result['hours'] = t.hour
        result['minutes'] = t.minute
        result['seconds'] = t.second
        result['microseconds'] = t.microsecond

    return interval(**result)

def typecast_numeric(typ, value):
    return Decimal(value)

def typecast_binary(typ, value):
    return Binary(value)

def typecast_bool_ary(typ, value):
    value = str(value)[1:-1]
    if not value:
        return []
    return [ e == 't' for e in value.split(',') ]

def typecast_int_ary(typ, value):
    value = str(value)[1:-1]
    if not value:
        return []
    return [ int(e) for e in value.split(',') ]

import csv
def typecast_str_ary(typ, value):
    value = str(value)[1:-1]
    if not value:
        return []
    reader = csv.reader([value], doublequote=False, escapechar='\\')
    return [ unicode(e, 'utf-8') for e in reader.next() ]

default_typecasts = {
    'date': typecast_date,
    'datetime': typecast_datetime,
    'time': typecast_time,
    'interval': typecast_interval,
    'numeric': typecast_numeric,
    1000: typecast_bool_ary,
    1007: typecast_int_ary,
    1009: typecast_str_ary,
    INET_TYPE_OID: pg_typed_value,
    CIDR_TYPE_OID: pg_typed_value,
}

def typecast(typ, casts, value):
    if value is None:
        return value
    if typ in casts:
        return casts[typ](typ, value)
    return value

# Silence warnings about array-conversions which we do here.
for key in default_typecasts.keys():
    if not isinstance(key, int):
        continue
    warnings.filterwarnings('ignore',
                            'Unknown datatype %d processed as string' % key,
                            RuntimeWarning)

### encode 'format'-encoded placeholders as PostgreSQL $n placeholders
placeholder_re = re.compile(r'(%.)')
def encode_sql(sql):
    # FIXME - Should be possible to disable this warning.
    if '$1' in sql:
        warnings.warn('PostgreSQL-style bind-parameters deprecated',
                      stacklevel=3)

    j, pieces = 0, []
    for piece in placeholder_re.split(sql):
        if piece == '%s':
            j += 1
            piece = '$%d' % j
        elif piece == '%%':
            piece = '%'
        elif piece.startswith('%'):
            warnings.warn('Unescaped % in SQL',
                          stacklevel=3)
        pieces.append(piece)
    return ''.join(pieces)

### cursor object
class Cursor(object):
    def __init__(self, src, connection):
        self._source = src
        self.connection = connection
        self.typecasts = connection.typecasts

    def _not_closed(self):
        self.connection._not_closed()
        if self._source is None:
            raise Error('Cursor already closed')

    def _cleanup(self):
        pass

    def close(self):
        if self._source:
            self._cleanup()
            self._source.close()
            self._source = None

    def __del__(self):
        if self._source:
            self.close()

    def _start(self, operation = None):
        self._not_closed()
        transaction = self._source.connection.transaction
        if transaction == TRANS_UNKNOWN:
            raise DatabaseError("Invalid/Unknown database connection")
        if transaction == TRANS_INERROR:
            raise ProgrammingError("Invalid transaction state. "
                                   "Exception not cleared by COMMIT or ROLLBACK")
        # do we need to start a transaction?
        if transaction in [TRANS_INTRANS, TRANS_ACTIVE]:
            return
        if operation is None:
            return
        op = operation.strip()[:6].lower()
        assert transaction <> TRANS_IDLE

    # if parameters are passed in, we'll attempt to bind them
    def execute(self, operation, params=[]):
        self._start(operation)
        operation = encode_sql(operation)
        params = self.connection.encode_params(params)
        ret = self._source.execute(operation, params)
        if isinstance(ret, int):
            return ret
        return self

    def executemany(self, operation, param_seq):
        self._start(operation)
        operation = encode_sql(operation)
        params_seq = (self.connection.encode_params(params)
                      for params in param_seq)
        ret = self._source.executemany(operation, param_seq)
        return self

    def fetchone(self):
        return self._typecast(self._source.fetchone())

    def fetchall(self):
        return map(self._typecast, self._source.fetchall())

    def __manyiter(self, size, fetchone):
        for x in xrange(size):
            val = fetchone()
            if val is None:
                raise StopIteration
            yield self._typecast(val)
    def fetchmany(self, size = None):
        if size is None:
            size = self.arraysize
        return list(self.__manyiter(size, self._source.fetchone))

    def _typecast(self, row):
        if row is None:
            return
        types = zip(*self.description)[1]
        casts = self.typecasts
        return tuple(typecast(t, casts, v) for t, v in zip(types, row))

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, col=0):
        pass

    # iterator support
    def __iter__(self):
        return self
    def next(self):
        item = self.fetchone()
        if item is None:
            raise StopIteration
        else:
            return item

    # Attributes
    def __getattr__(self, name):
        return getattr(self._source, name)

    def get_arraysize(self):
        return self._soure.arraysize
    def set_arraysize(self, value):
        self._source.arraysize = value
    arraysize = property(get_arraysize, set_arraysize)

# A cursor class for prepared statements
class PreparedCursor(Cursor):
    def __init__(self, *args):
        Cursor.__init__(self, *args)
        assert self._source.connection.transaction <> TRANS_IDLE

    # we require parameters since we've already bound a query
    def execute(self, params=[]):
        #self._start()
        self._not_closed()
        params = self.connection.encode_params(params)
        ret = self._source.execute(params)
        if isinstance(ret, int):
            return ret
        return self
    def executemany(self, param_seq):
        #self._start()
        self._not_closed()
        param_seq = (
            self.connection.encode_params(params)
            for params in param_seq
        )
        ret = self._source.executemany(param_seq)
        return self

# A cursor for large SELECTs that uses server side cursors
class IterCursor(Cursor):
    def __init__(self, source, *args):
        Cursor.__init__(self, source, *args)
        self.active = 0
        # we need a fairly random name for our cursor executions
        self.name = "c%ss%s" % (hex(abs(id(self))), hex(abs(id(source))))

    def _start(self, operation=None):
        self._cleanup()
        # We lie about which operation we're about to execute to force a start
        # transaction, which is required for 'without hold' on the cursor.
        Cursor._start(self, operation='insert')
        
    def _cleanup(self):
        if self.active and self._source.valid:
            self._source.execute("CLOSE %s" % self.name)
            self.active = 0

    def execute(self, query, params=[]):
        query = query.strip()
        if not query.lower().startswith("select"):
            return Cursor.execute(self, query, params)

        self._start()
        query = encode_sql(query)
        query = "DECLARE %s NO SCROLL CURSOR WITHOUT HOLD FOR\n%s" \
                % (self.name, query)
        params = self.connection.encode_params(params)

        ret = self._source.execute(query, params)
        self.active = 1
        return ret

    def __fetchone(self):
        # if this is not an active cursor, passthrough
        if self.active:
            self._source.query("FETCH NEXT FROM %s" % self.name)
    def fetchone(self):
        self.__fetchone()
        return Cursor.fetchone(self)

    def __fetchall(self):
        if self.active:
            self._source.execute("FETCH ALL FROM %s" % self.name)
    def fetchall(self):
        self.__fetchall()
        return Cursor.fetchall(self)

    def __fetchmany(self, size):
        if size is None:
            size = self.arraysize
        if self.active:
            self._source.execute("FETCH %d FROM %s" % (size, self.name))
    def fetchmany(self, size = None):
        self.__fetchmany(size)
        # if we're a server side cursor, retrieve all the results from the fetch()
        if self.active:
            return Cursor.fetchall(self)
        return Cursor.fetchmany(self, size)

### connection object
class Database(object):
    def __init__(self, cnx):
        self.__cnx = cnx
        self.typecasts = default_typecasts.copy()
        self.typecasts['string'] = self.typecast_string
        self.encoding = 'utf-8'
        self.__cnx.execute('BEGIN')
        # for prepared statement cache
        self.__cache = {}

    def _not_closed(self):
        if self.__cnx is None:
            raise Error('Connection already closed')

    def __del__(self):
        if self.__cnx is not None:
            self.close()
        del self.__cache
        del self.__cnx

    def encode_params(self, params):
        encoded = []
        for value in params:
            if isinstance(value, unicode):
                value = value.encode(self._encoding)
            encoded.append(value)
        return encoded

    def typecast_string(self, typ, s):
        return s.decode(self._encoding)

    def close(self):
        # deallocate statements
        self._not_closed()
        if self.__cnx.transaction in [TRANS_INERROR, TRANS_INTRANS]:
            self.__cnx.execute("ROLLBACK")
        for src, name in self.__cache.itervalues():
            if not src.valid:
                continue
            try:
                self.__cnx.execute("DEALLOCATE %s" % (name,))
            except pgsql.ProgrammingError:
                self.__cnx.execute("ROLLBACK")
            src.close()
        self.__cache = {}
        self.__cnx.close()
        self.__cnx = None

    # Postgresql autocommits everything not inside a transaction
    # block, so to simulate autocommit=off, we have to start new
    # transactions as soon as we flush them
    def commit(self):
        self._not_closed()
        self.__cnx.execute("COMMIT")
        self.__cnx.execute('BEGIN')

    def rollback(self):
        self._not_closed()
        self.__cnx.execute("ROLLBACK")
        self.__cnx.execute('BEGIN')

    def execute(self, query, params=[]):
        self._not_closed()
        query = encode_sql(query)
        params = self.encode_params(params)
        ret = self.__cnx.execute(query, params)
        if isinstance(ret, int):
            return ret
        return Cursor(ret, self)

    def cursor(self):
        self._not_closed()
        src = self.__cnx.source()
        return Cursor(src, self)

    def itercursor(self):
        '''Create a iterator (server-side) cursor.

        Iterator cursors work exactly like normal cursors.'''
        self._not_closed()
        src = self.__cnx.source()
        return IterCursor(src, self)

    def prepare(self, sql):
        '''Create a prepared statement.

        The preapred statement can be used like a normal cursor, with
        the exception that the execute method only accepts values for
        bind parameters.'''
        self._not_closed()
        sql = encode_sql(sql).strip()
        src, name = self.__cache.get(sql, (None, None))
        if src is None:
            name = "prep%d" % (len(self.__cache),)
            src = self.__cnx.prepare(sql, name)
            self.__cache[sql] = (src, name)
        return PreparedCursor(src, self)

    # FIXME - Should this just be a connect parameter instead?
    def get_encoding(self):
        return self._encoding
    def set_encoding(self, e):
        self.execute('SET SESSION client_encoding = "%s"' % e)
        self._encoding = e
    encoding = property(get_encoding, set_encoding)

    def copy_in(self, sql_stmt, iterable):
        '''Execute a postgresql COPY IN statement.

        Execute ``sql_stmt`` and send all byte-strings from iterable as
        the COPY IN data.

        Notice that if there are any unicode-strings in iterable, which
        aren't all ASCII, this will blow up horribly.'''

        self.execute(sql_stmt)
        for data in iterable:
            self.__cnx.put_copy_data(data)
        return self.__cnx.put_copy_end()

### module interface
def connect(database=None, user=None, password=None,
            host=None, port=-1, opt=None, tty=None):
    '''Connect to a PostgreSQL database.'''
    cnx = _pgsql.connect(database, user, password, host, port, opt, tty)
    return Database(cnx)

### Type-code comparators
class TypeCode:
    '''My job is to compare equal to those type-codes (from
    cursor.description) which I represent.'''
    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1

STRING = TypeCode('string')
BINARY = TypeCode('binary')
NUMBER = TypeCode('integer', 'long', 'float', 'numeric', 'money', 'bool')
DATETIME = TypeCode('date', 'datetime', 'time', 'interval')
ROWID = TypeCode('oid')

del TypeCode

# Type-constructors
def split_second(s):
    '''Split floating-point second into whole seconds and microseconds.'''
    ms = 0
    if isinstance(s, float):
        ms, s = modf(s)
    return int(s), int(floor(ms * 1000000.0))

def Date(year, month, day):
    return date(year, month, day)
def Time(hour, minute, second):
    s, ms = split_second(second)
    return time(hour, minute, s, ms)
def Timestamp(year, month, day, hour, minute, second):
    s, ms = split_second(second)
    return datetime(year, month, day, hour, minute, s, ms)

class Binary:
    __binary__ = True
    __pgsql_typeoid__ = BYTEA_TYPE_OID
    def __init__(self, s):
        self.value = str(s)
    def __str__(self):
        return self.value
    def __repr__(self):
        return 'Binary(%r)' % self.value

def DateFromTicks(ticks):
    return apply(Date, localtime(ticks)[:3])
def TimeFromTicks(ticks):
    return apply(Time, localtime(ticks)[3:6])
def TimestampFromTicks(ticks):
    return apply(Timestamp, localtime(ticks)[:6])
