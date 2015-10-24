#!/usr/bin/env python
#
# pg.py
#
# Written by D'Arcy J.M. Cain
# Improved by Christoph Zwerschke
#
# $Id: pg.py,v 1.77 2008/12/30 16:40:00 darcy Exp $
#

"""PyGreSQL classic interface.

This pg module implements some basic database management stuff.
It includes the _pg module and builds on it, providing the higher
level wrapper class named DB with addtional functionality.
This is known as the "classic" ("old style") PyGreSQL interface.
For a DB-API 2 compliant interface use the newer pgdb module.

"""

from _pg import *
try:
    frozenset
except NameError: # Python < 2.4
    from sets import ImmutableSet as frozenset
try:
    from decimal import Decimal
    set_decimal(Decimal)
except ImportError:
    pass # Python < 2.4


# Auxiliary functions which are independent from a DB connection:

def _is_quoted(s):
    """Check whether this string is a quoted identifier."""
    s = s.replace('_', 'a')
    return not s.isalnum() or s[:1].isdigit() or s != s.lower()

def _is_unquoted(s):
    """Check whether this string is an unquoted identifier."""
    s = s.replace('_', 'a')
    return s.isalnum() and not s[:1].isdigit()

def _split_first_part(s):
    """Split the first part of a dot separated string."""
    s = s.lstrip()
    if s[:1] == '"':
        p = []
        s = s.split('"', 3)[1:]
        p.append(s[0])
        while len(s) == 3 and s[1] == '':
            p.append('"')
            s = s[2].split('"', 2)
            p.append(s[0])
        p = [''.join(p)]
        s = '"'.join(s[1:]).lstrip()
        if s:
            if s[:0] == '.':
                p.append(s[1:])
            else:
                s = _split_first_part(s)
                p[0] += s[0]
                if len(s) > 1:
                    p.append(s[1])
    else:
        p = s.split('.', 1)
        s = p[0].rstrip()
        if _is_unquoted(s):
            s = s.lower()
        p[0] = s
    return p

def _split_parts(s):
    """Split all parts of a dot separated string."""
    q = []
    while s:
        s = _split_first_part(s)
        q.append(s[0])
        if len(s) < 2:
            break
        s = s[1]
    return q

def _join_parts(s):
    """Join all parts of a dot separated string."""
    return '.'.join([_is_quoted(p) and '"%s"' % p or p for p in s])

def _oid_key(qcl):
    """Build oid key from qualified class name."""
    return 'oid(%s)' % qcl


# The PostGreSQL database connection interface:

class DB(object):
    """Wrapper class for the _pg connection type."""

    def __init__(self, *args, **kw):
        """Create a new connection.

        You can pass either the connection parameters or an existing
        _pg or pgdb connection. This allows you to use the methods
        of the classic pg interface with a DB-API 2 pgdb connection.

        """
        if not args and len(kw) == 1:
            db = kw.get('db')
        elif not kw and len(args) == 1:
            db = args[0]
        else:
            db = None
        if db:
            if isinstance(db, DB):
                db = db.db
            else:
                try:
                    db = db._cnx
                except AttributeError:
                    pass
        if not db or not hasattr(db, 'db') or not hasattr(db, 'query'):
            db = connect(*args, **kw)
            self._closeable = 1
        else:
            self._closeable = 0
        self.db = db
        self.dbname = db.db
        self._attnames = {}
        self._pkeys = {}
        self._privileges = {}
        self._args = args, kw
        self.debug = None # For debugging scripts, this can be set
            # * to a string format specification (e.g. in CGI set to "%s<BR>"),
            # * to a file object to write debug statements or
            # * to a callable object which takes a string argument.

    def __getattr__(self, name):
        # All undefined members are the same as in the underlying pg connection:
        if self.db:
            return getattr(self.db, name)
        else:
            raise InternalError('Connection is not valid')

    # Auxiliary methods

    def _do_debug(self, s):
        """Print a debug message."""
        if self.debug:
            if isinstance(self.debug, basestring):
                print self.debug % s
            elif isinstance(self.debug, file):
                print >> self.debug, s
            elif callable(self.debug):
                self.debug(s)

    def _quote_text(self, d):
        """Quote text value."""
        if not isinstance(d, basestring):
            d = str(d)
        return "'%s'" % self.escape_string(d)

    _bool_true = frozenset('t true 1 y yes on'.split())

    def _quote_bool(self, d):
        """Quote boolean value."""
        if isinstance(d, basestring):
            if not d:
                return 'NULL'
            d = d.lower() in self._bool_true
        else:
            d = bool(d)
        return ("'f'", "'t'")[d]

    _date_literals = frozenset('current_date current_time'
        ' current_timestamp localtime localtimestamp'.split())

    def _quote_date(self, d):
        """Quote date value."""
        if not d:
            return 'NULL'
        if isinstance(d, basestring) and d.lower() in self._date_literals:
            return d
        return self._quote_text(d)

    def _quote_num(self, d):
        """Quote numeric value."""
        if not d and d != 0:
            return 'NULL'
        return str(d)

    def _quote_money(self, d):
        """Quote money value."""
        if not d:
            return 'NULL'
        return "'%.2f'" % float(d)

    _quote_funcs = dict( # quote methods for each type
        text=_quote_text, bool=_quote_bool, date=_quote_date,
        int=_quote_num, num=_quote_num, float=_quote_num,
        money=_quote_money)

    def _quote(self, d, t):
        """Return quotes if needed."""
        if d is None:
            return 'NULL'
        try:
            quote_func = self._quote_funcs[t]
        except KeyError:
            quote_func = self._quote_funcs['text']
        return quote_func(self, d)

    def _split_schema(self, cl):
        """Return schema and name of object separately.

        This auxiliary function splits off the namespace (schema)
        belonging to the class with the name cl. If the class name
        is not qualified, the function is able to determine the schema
        of the class, taking into account the current search path.

        """
        s = _split_parts(cl)
        if len(s) > 1: # name already qualfied?
            # should be database.schema.table or schema.table
            if len(s) > 3:
                raise ProgrammingError('Too many dots in class name %s' % cl)
            schema, cl = s[-2:]
        else:
            cl = s[0]
            # determine search path
            q = 'SELECT current_schemas(TRUE)'
            schemas = self.db.query(q).getresult()[0][0][1:-1].split(',')
            if schemas: # non-empty path
                # search schema for this object in the current search path
                q = ' UNION '.join(
                    ["SELECT %d::integer AS n, '%s'::name AS nspname"
                        % s for s in enumerate(schemas)])
                q = ("SELECT nspname FROM pg_class"
                    " JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid"
                    " JOIN (%s) AS p USING (nspname)"
                    " WHERE pg_class.relname = '%s'"
                    " ORDER BY n LIMIT 1" % (q, cl))
                schema = self.db.query(q).getresult()
                if schema: # schema found
                    schema = schema[0][0]
                else: # object not found in current search path
                    schema = 'public'
            else: # empty path
                schema = 'public'
        return schema, cl

    def _add_schema(self, cl):
        """Ensure that the class name is prefixed with a schema name."""
        return _join_parts(self._split_schema(cl))

    # Public methods

    # escape_string and escape_bytea exist as methods,
    # so we define unescape_bytea as a method as well
    unescape_bytea = staticmethod(unescape_bytea)

    def close(self):
        """Close the database connection."""
        # Wraps shared library function so we can track state.
        if self._closeable:
            if self.db:
                self.db.close()
                self.db = None
            else:
                raise InternalError('Connection already closed')

    def reset(self):
        """Reset connection with current parameters.

        All derived queries and large objects derived from this connection
        will not be usable after this call.

        """
        self.db.reset()

    def reopen(self):
        """Reopen connection to the database.

        Used in case we need another connection to the same database.
        Note that we can still reopen a database that we have closed.

        """
        # There is no such shared library function.
        if self._closeable:
            db = connect(*self._args[0], **self._args[1])
            if self.db:
                self.db.close()
            self.db = db

    def query(self, qstr):
        """Executes a SQL command string.

        This method simply sends a SQL query to the database. If the query is
        an insert statement that inserted exactly one row into a table that
        has OIDs, the return value is the OID of the newly inserted row.
        If the query is an update or delete statement, or an insert statement
        that did not insert exactly one row in a table with OIDs, then the
        numer of rows affected is returned as a string. If it is a statement
        that returns rows as a result (usually a select statement, but maybe
        also an "insert/update ... returning" statement), this method returns
        a pgqueryobject that can be accessed via getresult() or dictresult()
        or simply printed. Otherwise, it returns `None`.

        """
        # Wraps shared library function for debugging.
        if not self.db:
            raise InternalError('Connection is not valid')
        self._do_debug(qstr)
        return self.db.query(qstr)

    def pkey(self, cl, newpkey=None):
        """This method gets or sets the primary key of a class.

        Composite primary keys are represented as frozensets. Note that
        this raises an exception if the table does not have a primary key.

        If newpkey is set and is not a dictionary then set that
        value as the primary key of the class.  If it is a dictionary
        then replace the _pkeys dictionary with a copy of it.

        """
        # First see if the caller is supplying a dictionary
        if isinstance(newpkey, dict):
            # make sure that all classes have a namespace
            self._pkeys = dict([
                ('.' in cl and cl or 'public.' + cl, pkey)
                for cl, pkey in newpkey.iteritems()])
            return self._pkeys

        qcl = self._add_schema(cl) # build fully qualified class name
        # Check if the caller is supplying a new primary key for the class
        if newpkey:
            self._pkeys[qcl] = newpkey
            return newpkey

        # Get all the primary keys at once
        if qcl not in self._pkeys:
            # if not found, check again in case it was added after we started
            self._pkeys = {}
            if self.server_version >= 80200:
                # the ANY syntax works correctly only with PostgreSQL >= 8.2
                any_indkey = "= ANY (pg_index.indkey)"
            else:
                any_indkey = "IN (%s)" % ', '.join(
                    ['pg_index.indkey[%d]' % i for i in range(16)])
            for r in self.db.query(
                "SELECT pg_namespace.nspname, pg_class.relname,"
                    " pg_attribute.attname FROM pg_class"
                " JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace"
                    " AND pg_namespace.nspname NOT LIKE 'pg_%'"
                " JOIN pg_attribute ON pg_attribute.attrelid = pg_class.oid"
                    " AND pg_attribute.attisdropped = 'f'"
                " JOIN pg_index ON pg_index.indrelid = pg_class.oid"
                    " AND pg_index.indisprimary = 't'"
                    " AND pg_attribute.attnum " + any_indkey).getresult():
                cl, pkey = _join_parts(r[:2]), r[2]
                self._pkeys.setdefault(cl, []).append(pkey)
            # (only) for composite primary keys, the values will be frozensets
            for cl, pkey in self._pkeys.iteritems():
                self._pkeys[cl] = len(pkey) > 1 and frozenset(pkey) or pkey[0]
            self._do_debug(self._pkeys)

        # will raise an exception if primary key doesn't exist
        return self._pkeys[qcl]

    def get_databases(self):
        """Get list of databases in the system."""
        return [s[0] for s in
            self.db.query('SELECT datname FROM pg_database').getresult()]

    def get_relations(self, kinds=None):
        """Get list of relations in connected database of specified kinds.

            If kinds is None or empty, all kinds of relations are returned.
            Otherwise kinds can be a string or sequence of type letters
            specifying which kind of relations you want to list.

        """
        where = kinds and "pg_class.relkind IN (%s) AND" % ','.join(
            ["'%s'" % x for x in kinds]) or ''
        return map(_join_parts, self.db.query(
            "SELECT pg_namespace.nspname, pg_class.relname "
            "FROM pg_class "
            "JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace "
            "WHERE %s pg_class.relname !~ '^Inv' AND "
                "pg_class.relname !~ '^pg_' "
            "ORDER BY 1, 2" % where).getresult())

    def get_tables(self):
        """Return list of tables in connected database."""
        return self.get_relations('r')

    def get_attnames(self, cl, newattnames=None):
        """Given the name of a table, digs out the set of attribute names.

        Returns a dictionary of attribute names (the names are the keys,
        the values are the names of the attributes' types).
        If the optional newattnames exists, it must be a dictionary and
        will become the new attribute names dictionary.

        """
        if isinstance(newattnames, dict):
            self._attnames = newattnames
            return
        elif newattnames:
            raise ProgrammingError(
                'If supplied, newattnames must be a dictionary')
        cl = self._split_schema(cl) # split into schema and class
        qcl = _join_parts(cl) # build fully qualified name
        # May as well cache them:
        if qcl in self._attnames:
            return self._attnames[qcl]
        if qcl not in self.get_relations('rv'):
            raise ProgrammingError('Class %s does not exist' % qcl)
        t = {}
        for att, typ in self.db.query("SELECT pg_attribute.attname,"
            " pg_type.typname FROM pg_class"
            " JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid"
            " JOIN pg_attribute ON pg_attribute.attrelid = pg_class.oid"
            " JOIN pg_type ON pg_type.oid = pg_attribute.atttypid"
            " WHERE pg_namespace.nspname = '%s' AND pg_class.relname = '%s'"
            " AND (pg_attribute.attnum > 0 or pg_attribute.attname = 'oid')"
            " AND pg_attribute.attisdropped = 'f'"
                % cl).getresult():
            if typ.startswith('bool'):
                t[att] = 'bool'
            elif typ.startswith('abstime'):
                t[att] = 'date'
            elif typ.startswith('date'):
                t[att] = 'date'
            elif typ.startswith('interval'):
                t[att] = 'date'
            elif typ.startswith('timestamp'):
                t[att] = 'date'
            elif typ.startswith('oid'):
                t[att] = 'int'
            elif typ.startswith('int'):
                t[att] = 'int'
            elif typ.startswith('float'):
                t[att] = 'float'
            elif typ.startswith('numeric'):
                t[att] = 'num'
            elif typ.startswith('money'):
                t[att] = 'money'
            else:
                t[att] = 'text'
        self._attnames[qcl] = t # cache it
        return self._attnames[qcl]

    def has_table_privilege(self, cl, privilege='select'):
        """Check whether current user has specified table privilege."""
        qcl = self._add_schema(cl)
        privilege = privilege.lower()
        try:
            return self._privileges[(qcl, privilege)]
        except KeyError:
            q = "SELECT has_table_privilege('%s', '%s')" % (qcl, privilege)
            ret = self.db.query(q).getresult()[0][0] == 't'
            self._privileges[(qcl, privilege)] = ret
            return ret

    def get(self, cl, arg, keyname=None):
        """Get a tuple from a database table or view.

        This method is the basic mechanism to get a single row.  The keyname
        that the key specifies a unique row.  If keyname is not specified
        then the primary key for the table is used.  If arg is a dictionary
        then the value for the key is taken from it and it is modified to
        include the new values, replacing existing values where necessary.
        For a composite key, keyname can also be a sequence of key names.
        The OID is also put into the dictionary if the table has one, but
        in order to allow the caller to work with multiple tables, it is
        munged as oid(schema.table).

        """
        if cl.endswith('*'): # scan descendant tables?
            cl = cl[:-1].rstrip() # need parent table name
        # build qualified class name
        qcl = self._add_schema(cl)
        # To allow users to work with multiple tables,
        # we munge the name of the "oid" the key
        qoid = _oid_key(qcl)
        if not keyname:
            # use the primary key by default
            try:
               keyname = self.pkey(qcl)
            except KeyError:
               raise ProgrammingError('Class %s has no primary key' % qcl)
        # We want the oid for later updates if that isn't the key
        if keyname == 'oid':
            if isinstance(arg, dict):
                if qoid not in arg:
                    raise ProgrammingError('%s not in arg' % qoid)
            else:
                arg = {qoid: arg}
            where = 'oid = %s' % arg[qoid]
            attnames = '*'
        else:
            attnames = self.get_attnames(qcl)
            if isinstance(keyname, basestring):
                keyname = (keyname,)
            if not isinstance(arg, dict):
                if len(keyname) > 1:
                    raise ProgrammingError('Composite key needs dict as arg')
                arg = dict([(k, arg) for k in keyname])
            where = ' AND '.join(['%s = %s'
                % (k, self._quote(arg[k], attnames[k])) for k in keyname])
            attnames = ', '.join(attnames)
        q = 'SELECT %s FROM %s WHERE %s LIMIT 1' % (attnames, qcl, where)
        self._do_debug(q)
        res = self.db.query(q).dictresult()
        if not res:
            raise DatabaseError('No such record in %s where %s' % (qcl, where))
        for att, value in res[0].iteritems():
            arg[att == 'oid' and qoid or att] = value
        return arg

    def insert(self, cl, d=None, **kw):
        """Insert a tuple into a database table.

        This method inserts a row into a table.  If a dictionary is
        supplied it starts with that.  Otherwise it uses a blank dictionary.
        Either way the dictionary is updated from the keywords.

        The dictionary is then, if possible, reloaded with the values actually
        inserted in order to pick up values modified by rules, triggers, etc.

        Note: The method currently doesn't support insert into views
        although PostgreSQL does.

        """
        qcl = self._add_schema(cl)
        qoid = _oid_key(qcl)
        if d is None:
            d = {}
        d.update(kw)
        attnames = self.get_attnames(qcl)
        names, values = [], []
        for n in attnames:
            if n != 'oid' and n in d:
                names.append('"%s"' % n)
                values.append(self._quote(d[n], attnames[n]))
        names, values = ', '.join(names), ', '.join(values)
        selectable = self.has_table_privilege(qcl)
        if selectable and self.server_version >= 80200:
            ret = ' RETURNING %s*' % ('oid' in attnames and 'oid, ' or '')
        else:
            ret = ''
        q = 'INSERT INTO %s (%s) VALUES (%s)%s' % (qcl, names, values, ret)
        self._do_debug(q)
        res = self.db.query(q)
        if ret:
            res = res.dictresult()
            for att, value in res[0].iteritems():
                d[att == 'oid' and qoid or att] = value
        elif isinstance(res, int):
            d[qoid] = res
            if selectable:
                self.get(qcl, d, 'oid')
        elif selectable:
            if qoid in d:
                self.get(qcl, d, 'oid')
            else:
                try:
                    self.get(qcl, d)
                except ProgrammingError:
                    pass # table has no primary key
        return d

    def update(self, cl, d=None, **kw):
        """Update an existing row in a database table.

        Similar to insert but updates an existing row.  The update is based
        on the OID value as munged by get or passed as keyword, or on the
        primary key of the table.  The dictionary is modified, if possible,
        to reflect any changes caused by the update due to triggers, rules,
        default values, etc.

        """
        # Update always works on the oid which get returns if available,
        # otherwise use the primary key.  Fail if neither.
        # Note that we only accept oid key from named args for safety
        qcl = self._add_schema(cl)
        qoid = _oid_key(qcl)
        if 'oid' in kw:
            kw[qoid] = kw['oid']
            del kw['oid']
        if d is None:
            d = {}
        d.update(kw)
        attnames = self.get_attnames(qcl)
        if qoid in d:
            where = 'oid = %s' % d[qoid]
            keyname = ()
        else:
            try:
                keyname = self.pkey(qcl)
            except KeyError:
                raise ProgrammingError('Class %s has no primary key' % qcl)
            if isinstance(keyname, basestring):
                keyname = (keyname,)
            try:
                where = ' AND '.join(['%s = %s'
                    % (k, self._quote(d[k], attnames[k])) for k in keyname])
            except KeyError:
                raise ProgrammingError('Update needs primary key or oid.')
        values = []
        for n in attnames:
            if n in d and n not in keyname:
                values.append('%s = %s' % (n, self._quote(d[n], attnames[n])))
        if not values:
            return d
        values = ', '.join(values)
        selectable = self.has_table_privilege(qcl)
        if selectable and self.server_version >= 880200:
            ret = ' RETURNING %s*' % ('oid' in attnames and 'oid, ' or '')
        else:
            ret = ''
        q = 'UPDATE %s SET %s WHERE %s%s' % (qcl, values, where, ret)
        self._do_debug(q)
        res = self.db.query(q)
        if ret:
            res = self.db.query(q).dictresult()
            for att, value in res[0].iteritems():
                d[att == 'oid' and qoid or att] = value
        else:
            self.db.query(q)
            if selectable:
                if qoid in d:
                    self.get(qcl, d, 'oid')
                else:
                    self.get(qcl, d)
        return d

    def clear(self, cl, a=None):
        """

        This method clears all the attributes to values determined by the types.
        Numeric types are set to 0, Booleans are set to 'f', and everything
        else is set to the empty string.  If the array argument is present,
        it is used as the array and any entries matching attribute names are
        cleared with everything else left unchanged.

        """
        # At some point we will need a way to get defaults from a table.
        qcl = self._add_schema(cl)
        if a is None:
            a = {} # empty if argument is not present
        attnames = self.get_attnames(qcl)
        for n, t in attnames.iteritems():
            if n == 'oid':
                continue
            if t in ('int', 'float', 'num', 'money'):
                a[n] = 0
            elif t == 'bool':
                a[n] = 'f'
            else:
                a[n] = ''
        return a

    def delete(self, cl, d=None, **kw):
        """Delete an existing row in a database table.

        This method deletes the row from a table.  It deletes based on the
        OID value as munged by get or passed as keyword, or on the primary
        key of the table.  The return value is the number of deleted rows
        (i.e. 0 if the row did not exist and 1 if the row was deleted).

        """
        # Like update, delete works on the oid.
        # One day we will be testing that the record to be deleted
        # isn't referenced somewhere (or else PostgreSQL will).
        # Note that we only accept oid key from named args for safety
        qcl = self._add_schema(cl)
        qoid = _oid_key(qcl)
        if 'oid' in kw:
            kw[qoid] = kw['oid']
            del kw['oid']
        if d is None:
            d = {}
        d.update(kw)
        if qoid in d:
            where = 'oid = %s' % d[qoid]
        else:
            try:
                keyname = self.pkey(qcl)
            except KeyError:
                raise ProgrammingError('Class %s has no primary key' % qcl)
            if isinstance(keyname, basestring):
                keyname = (keyname,)
            attnames = self.get_attnames(qcl)
            try:
                where = ' AND '.join(['%s = %s'
                    % (k, self._quote(d[k], attnames[k])) for k in keyname])
            except KeyError:
                raise ProgrammingError('Delete needs primary key or oid.')
        q = 'DELETE FROM %s WHERE %s' % (qcl, where)
        self._do_debug(q)
        return int(self.db.query(q))


# if run as script, print some information

if __name__ == '__main__':
    print 'PyGreSQL version', version
    print
    print __doc__
