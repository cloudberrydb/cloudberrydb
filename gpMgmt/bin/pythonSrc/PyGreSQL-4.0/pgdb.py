#!/usr/bin/env python
#
# pgdb.py
#
# Written by D'Arcy J.M. Cain
#
# $Id: pgdb.py,v 1.54 2008/11/23 14:32:18 cito Exp $
#

"""pgdb - DB-API 2.0 compliant module for PygreSQL.

(c) 1999, Pascal Andre <andre@via.ecp.fr>.
See package documentation for further information on copyright.

Inline documentation is sparse.
See DB-API 2.0 specification for usage information:
http://www.python.org/peps/pep-0249.html

Basic usage:

    pgdb.connect(connect_string) # open a connection
    # connect_string = 'host:database:user:password:opt:tty'
    # All parts are optional. You may also pass host through
    # password as keyword arguments. To pass a port,
    # pass it in the host keyword parameter:
    pgdb.connect(host='localhost:5432')

    connection.cursor() # open a cursor

    cursor.execute(query[, params])
    # Execute a query, binding params (a dictionary) if they are
    # passed. The binding syntax is the same as the % operator
    # for dictionaries, and no quoting is done.

    cursor.executemany(query, list of params)
    # Execute a query many times, binding each param dictionary
    # from the list.

    cursor.fetchone() # fetch one row, [value, value, ...]

    cursor.fetchall() # fetch all rows, [[value, value, ...], ...]

    cursor.fetchmany([size])
    # returns size or cursor.arraysize number of rows,
    # [[value, value, ...], ...] from result set.
    # Default cursor.arraysize is 1.

    cursor.description # returns information about the columns
    #	[(column_name, type_name, display_size,
    #		internal_size, precision, scale, null_ok), ...]
    # Note that precision, scale and null_ok are not implemented.

    cursor.rowcount # number of rows available in the result set
    # Available after a call to execute.

    connection.commit() # commit transaction

    connection.rollback() # or rollback transaction

    cursor.close() # close the cursor

    connection.close() # close the connection

"""

from _pg import *
import time
try:
    frozenset
except NameError: # Python < 2.4
    from sets import ImmutableSet as frozenset
from datetime import datetime, timedelta
try: # use Decimal if available
    from decimal import Decimal
    set_decimal(Decimal)
except ImportError: # otherwise (Python < 2.4)
    Decimal = float # use float instead of Decimal


### Module Constants

# compliant with DB SIG 2.0
apilevel = '2.0'

# module may be shared, but not connections
threadsafety = 1

# this module use extended python format codes
paramstyle = 'pyformat'


### Internal Types Handling

def decimal_type(decimal_type=None):
    """Get or set global type to be used for decimal values."""
    global Decimal
    if decimal_type is not None:
        Decimal = decimal_type
        set_decimal(decimal_type)
    return Decimal


def _cast_bool(value):
    return value[:1] in ['t', 'T']


def _cast_money(value):
    return Decimal(''.join(filter(
        lambda v: v in '0123456789.-', value)))


_cast = {'bool': _cast_bool,
    'int2': int, 'int4': int, 'serial': int,
    'int8': long, 'oid': long, 'oid8': long,
    'float4': float, 'float8': float,
    'numeric': Decimal, 'money': _cast_money}


class pgdbTypeCache(dict):
    """Cache for database types."""

    def __init__(self, cnx):
        """Initialize type cache for connection."""
        super(pgdbTypeCache, self).__init__()
        self._src = cnx.source()

    def typecast(typ, value):
        """Cast value to database type."""
        if value is None:
            # for NULL values, no typecast is necessary
            return None
        cast = _cast.get(typ)
        if cast is None:
            # no typecast available or necessary
            return value
        else:
            return cast(value)
    typecast = staticmethod(typecast)

    def getdescr(self, oid):
        """Get name of database type with given oid."""
        try:
            return self[oid]
        except KeyError:
            self._src.execute(
                "SELECT typname, typlen "
                "FROM pg_type WHERE oid=%s" % oid)
            res = self._src.fetch(1)[0]
            # The column name is omitted from the return value.
            # It will have to be prepended by the caller.
            res = (res[0], None, int(res[1]),
                None, None, None)
            self[oid] = res
            return res


class _quotedict(dict):
    """Dictionary with auto quoting of its items.

    The quote attribute must be set to the desired quote function.

    """

    def __getitem__(self, key):
        return self.quote(super(_quotedict, self).__getitem__(key))


### Cursor Object

class pgdbCursor(object):
    """Cursor Object."""

    def __init__(self, dbcnx):
        """Create a cursor object for the database connection."""
        self.connection = self._dbcnx = dbcnx
        self._cnx = dbcnx._cnx
        self._type_cache = dbcnx._type_cache
        self._src = self._cnx.source()
        self.description = None
        self.rowcount = -1
        self.arraysize = 1
        self.lastrowid = None

    def __iter__(self):
        """Return self to make cursors compatible to the iteration protocol."""
        return self

    def _quote(self, val):
        """Quote value depending on its type."""
        if isinstance(val, datetime):
            val = str(val)
        elif isinstance(val, unicode):
            val = val.encode( 'utf8' )
        if isinstance(val, str):
            val = "'%s'" % self._cnx.escape_string(val)
        elif isinstance(val, (int, long, float)):
            pass
        elif val is None:
            val = 'NULL'
        elif isinstance(val, (list, tuple)):
            val = '(%s)' % ','.join(map(lambda v: str(self._quote(v)), val))
        elif Decimal is not float and isinstance(val, Decimal):
            pass
        elif hasattr(val, '__pg_repr__'):
            val = val.__pg_repr__()
        else:
            raise InterfaceError(
                'do not know how to handle type %s' % type(val))
        return val

    def _quoteparams(self, string, params):
        """Quote parameters.

        This function works for both mappings and sequences.

        """
        if isinstance(params, dict):
            params = _quotedict(params)
            params.quote = self._quote
        else:
            params = tuple(map(self._quote, params))
        return string % params

    def row_factory(row):
        """Process rows before they are returned.

        You can overwrite this with a custom row factory,
        e.g. a dict factory:

        class myCursor(pgdb.pgdbCursor):
            def cursor.row_factory(self, row):
                d = {}
                for idx, col in enumerate(self.description):
                    d[col[0]] = row[idx]
                return d
        cursor = myCursor(cnx)

        """
        return row
    row_factory = staticmethod(row_factory)

    def close(self):
        """Close the cursor object."""
        self._src.close()
        self.description = None
        self.rowcount = -1
        self.lastrowid = None

    def execute(self, operation, params=None):
        """Prepare and execute a database operation (query or command)."""
        # The parameters may also be specified as list of
        # tuples to e.g. insert multiple rows in a single
        # operation, but this kind of usage is deprecated:
        if (params and isinstance(params, list)
                and isinstance(params[0], tuple)):
            self.executemany(operation, params)
        else:
            # not a list of tuples
            self.executemany(operation, (params,))

    def executemany(self, operation, param_seq):
        """Prepare operation and execute it against a parameter sequence."""
        if not param_seq:
            # don't do anything without parameters
            return
        self.description = None
        self.rowcount = -1
        # first try to execute all queries
        totrows = 0
        sql = "BEGIN"
        try:
            if not self._dbcnx._tnx:
                try:
                    self._cnx.source().execute(sql)
                except Exception:
                    raise OperationalError("can't start transaction")
                self._dbcnx._tnx = True
            for params in param_seq:
                if params:
                    sql = self._quoteparams(operation, params)
                else:
                    sql = operation
                rows = self._src.execute(sql)
                if rows: # true if not DML
                    totrows += rows
                else:
                    self.rowcount = -1
        except Error, msg:
            raise DatabaseError("error '%s' in '%s'" % (msg, sql))
        except Exception, err:
            raise OperationalError("internal error in '%s': %s" % (sql, err))
        # then initialize result raw count and description
        if self._src.resulttype == RESULT_DQL:
            self.rowcount = self._src.ntuples
            getdescr = self._type_cache.getdescr
            coltypes = self._src.listinfo()
            self.description = [typ[1:2] + getdescr(typ[2]) for typ in coltypes]
            self.lastrowid = self._src.oidstatus()
        else:
            self.rowcount = totrows
            self.description = None
            self.lastrowid = self._src.oidstatus()

    def fetchone(self):
        """Fetch the next row of a query result set."""
        res = self.fetchmany(1, False)
        try:
            return res[0]
        except IndexError:
            return None

    def fetchall(self):
        """Fetch all (remaining) rows of a query result."""
        return self.fetchmany(-1, False)

    def fetchmany(self, size=None, keep=False):
        """Fetch the next set of rows of a query result.

        The number of rows to fetch per call is specified by the
        size parameter. If it is not given, the cursor's arraysize
        determines the number of rows to be fetched. If you set
        the keep parameter to true, this is kept as new arraysize.

        """
        if size is None:
            size = self.arraysize
        if keep:
            self.arraysize = size
        try:
            result = self._src.fetch(size)
        except Error, err:
            raise DatabaseError(str(err))
        row_factory = self.row_factory
        typecast = self._type_cache.typecast
        coltypes = [desc[1] for desc in self.description]
        return [row_factory([typecast(*args)
            for args in zip(coltypes, row)]) for row in result]

    def next(self):
        """Return the next row (support for the iteration protocol)."""
        res = self.fetchone()
        if res is None:
            raise StopIteration
        return res

    def nextset():
        """Not supported."""
        raise NotSupportedError("nextset() is not supported")
    nextset = staticmethod(nextset)

    def setinputsizes(sizes):
        """Not supported."""
        pass
    setinputsizes = staticmethod(setinputsizes)

    def setoutputsize(size, column=0):
        """Not supported."""
        pass
    setoutputsize = staticmethod(setoutputsize)


### Connection Objects

class pgdbCnx(object):
    """Connection Object."""

    # expose the exceptions as attributes on the connection object
    Error = Error
    Warning = Warning
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    InternalError = InternalError
    OperationalError = OperationalError
    ProgrammingError = ProgrammingError
    IntegrityError = IntegrityError
    DataError = DataError
    NotSupportedError = NotSupportedError

    def __init__(self, cnx):
        """Create a database connection object."""
        self._cnx = cnx # connection
        self._tnx = False # transaction state
        self._type_cache = pgdbTypeCache(cnx)
        try:
            self._cnx.source()
        except Exception:
            raise OperationalError("invalid connection")

    def close(self):
        """Close the connection object."""
        if self._cnx:
            self._cnx.close()
            self._cnx = None
        else:
            raise OperationalError("connection has been closed")

    def commit(self):
        """Commit any pending transaction to the database."""
        if self._cnx:
            if self._tnx:
                self._tnx = False
                try:
                    self._cnx.source().execute("COMMIT")
                except Exception:
                    raise OperationalError("can't commit")
        else:
            raise OperationalError("connection has been closed")

    def rollback(self):
        """Roll back to the start of any pending transaction."""
        if self._cnx:
            if self._tnx:
                self._tnx = False
                try:
                    self._cnx.source().execute("ROLLBACK")
                except Exception:
                    raise OperationalError("can't rollback")
        else:
            raise OperationalError("connection has been closed")

    def cursor(self):
        """Return a new Cursor Object using the connection."""
        if self._cnx:
            try:
                return pgdbCursor(self)
            except Exception:
                raise OperationalError("invalid connection")
        else:
            raise OperationalError("connection has been closed")


### Module Interface

_connect_ = connect

def connect(dsn=None,
        user=None, password=None,
        host=None, database=None):
    """Connects to a database."""
    # first get params from DSN
    dbport = -1
    dbhost = ""
    dbbase = ""
    dbuser = ""
    dbpasswd = ""
    dbopt = ""
    dbtty = ""
    try:
        params = dsn.split(":")
        dbhost = params[0]
        dbport = int(params[1])
        dbbase = params[2]
        dbuser = params[3]
        dbpasswd = params[4]
        dbopt = params[5]
        dbtty = params[6]
    except (AttributeError, IndexError, TypeError):
        pass

    # override if necessary
    if user is not None:
        dbuser = user
    if password is not None:
        dbpasswd = password
    if database is not None:
        dbbase = database
    if host is not None:
        try:
            params = host.split(":")
            dbhost = params[0]
            dbport = int(params[1])
        except (AttributeError, IndexError, TypeError, ValueError):
            pass

    # empty host is localhost
    if dbhost == "":
        dbhost = None
    if dbuser == "":
        dbuser = None

    # open the connection
    cnx = _connect_(dbbase, dbhost, dbport, dbopt,
        dbtty, dbuser, dbpasswd)
    return pgdbCnx(cnx)


### Types Handling

class pgdbType(frozenset):
    """Type class for a couple of PostgreSQL data types.

    PostgreSQL is object-oriented: types are dynamic.
    We must thus use type names as internal type codes.

    """

    if frozenset.__module__ == '__builtin__':
        def __new__(cls, values):
            if isinstance(values, basestring):
                values = values.split()
            return super(pgdbType, cls).__new__(cls, values)
    else: # Python < 2.4
        def __init__(self, values):
            if isinstance(values, basestring):
                values = values.split()
            super(pgdbType, self).__init__(values)

    def __eq__(self, other):
        if isinstance(other, basestring):
            return other in self
        else:
            return super(pgdbType, self).__eq__(other)

    def __ne__(self, other):
        if isinstance(other, basestring):
            return other not in self
        else:
            return super(pgdbType, self).__ne__(other)


# Mandatory type objects defined by DB-API 2 specs:

STRING = pgdbType('char bpchar name text varchar')
BINARY = pgdbType('bytea')
NUMBER = pgdbType('int2 int4 serial int8 float4 float8 numeric money')
DATETIME = pgdbType('date time timetz timestamp timestamptz datetime abstime'
    ' interval tinterval timespan reltime')
ROWID = pgdbType('oid oid8')


# Additional type objects (more specific):

BOOL = pgdbType('bool')
SMALLINT = pgdbType('int2')
INTEGER = pgdbType('int2 int4 int8 serial')
LONG = pgdbType('int8')
FLOAT = pgdbType('float4 float8')
NUMERIC = pgdbType('numeric')
MONEY = pgdbType('money')
DATE = pgdbType('date')
TIME = pgdbType('time timetz')
TIMESTAMP = pgdbType('timestamp timestamptz datetime abstime')
INTERVAL = pgdbType('interval tinterval timespan reltime')


# Mandatory type helpers defined by DB-API 2 specs:

def Date(year, month, day):
    """Construct an object holding a date value."""
    return datetime(year, month, day)

def Time(hour, minute, second):
    """Construct an object holding a time value."""
    return timedelta(hour, minute, second)

def Timestamp(year, month, day, hour, minute, second):
    """construct an object holding a time stamp value."""
    return datetime(year, month, day, hour, minute, second)

def DateFromTicks(ticks):
    """Construct an object holding a date value from the given ticks value."""
    return Date(*time.localtime(ticks)[:3])

def TimeFromTicks(ticks):
    """construct an object holding a time value from the given ticks value."""
    return Time(*time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    """construct an object holding a time stamp from the given ticks value."""
    return Timestamp(*time.localtime(ticks)[:6])

def Binary(value):
    """construct an object capable of holding a binary (long) string value."""
    return value


# If run as script, print some information:

if __name__ == '__main__':
    print 'PyGreSQL version', version
    print
    print __doc__
