#!/usr/bin/env python
#
# Copyright (c) Pivotal Inc 2014. All Rights Reserved.
#
# This script is intended to fix AO specific catalog issues
# introduced by an upgrade from 4.2.x to (4.3.0.0 - 4.3.2.0).  It
# scans each database for orphaned visimaps relations and missing
# pg_depend entries for visimap relations.
#
# Usage:
#
# To report AO specific catalog inconsistencies without making any
# changes:
#
#     fix_ao_upgrade.py --report
#
# To fix the inconsistencies, omit --report argument.


import getpass
from optparse import OptionParser
import re
import socket
import sys

try:
    from pygresql import pg
    # Use gppylib logger as it handles offers UTF-8 encoding and the
    # output format remains consistent with other GPDB tools.
    from gppylib import gplog
except ImportError, e:
    sys.exit("Could not import modules.  Please check that "
             "greenplum_path.sh is sourced.  Detail: " + str(e))

# Used for logging.
EXEC_NAME = "fix_ao_upgrade"

class ExceptionFilter(object):
    # This class filters out exception traces from stdout.  The
    # argument record below is of type logging.LogRecord.
    def filter(self, record):
        if record.exc_info is not None:
            return 0
        else:
            return 1

class GPDBException(Exception):
    pass

class GPDB(object):
    """Class to interact with GPDB using pygresql."""

    def __init__(self, dbname, host=None, port=None, user=None, passwd=None):
        args = {"dbname":dbname, "host":host, "user":user, "passwd":passwd}
        # pygresql comains if port is included in kwargs and its value is
        # None.  It allows all other kwargs to be None.
        if port is not None:
            args["port"] = port
        self.db = pg.DB(**args)
        is_super = self.db.query("SHOW is_superuser").getresult()
        if is_super[0][0] != "on":
            user = self.db.query("SELECT CURRENT_USER").getresult()
            self.db.close()
            raise GPDBException("'%s' is not a superuser." % user[0][0])
        # Ensure we only interact with a 4.3.x GPDB instance.
        version = self.db.query("SELECT version()").getresult()
        if re.match(".*Greenplum Database[ ]*4.3", version[0][0]) is None:
            self.db.close()
            raise GPDBException("GPDB is not running version 4.3.x")
        self.__systable_mods_set = False
        self.__catdml_created = False
        self.public_schema_is_created = False
        self.public_schema_oid = self.checkPublicSchemaOid()

    def checkPublicSchemaOid(self):
        oids = self.db.query("select oid from pg_namespace where nspname='public'").getresult()
        public_schema_oid = oids[0][0] if len(oids) > 0 else None
        if not public_schema_oid:
            self.db.query("create schema public")
            self.public_schema_is_created = True
            oids = self.db.query("select oid from pg_namespace where nspname='public'").getresult()
            public_schema_oid = oids[0][0] if len(oids) > 0 else None
        return public_schema_oid

    def allowSystableMods(self):
        if not self.__systable_mods_set:
            self.db.query("SET allow_system_table_mods='dml'")
            self.db.query("SET allow_segment_dml=on")
            res = self.db.query("SHOW allow_system_table_mods").getresult()
            if res[0][0].lower() != 'dml':
                self.cleanup()
                raise GPDBException("Failed to set system tables GUCs.")
            res = self.db.query("SHOW allow_segment_dml").getresult()
            if res[0][0].lower() != 'on':
                self.cleanup()
                raise GPDBException("Failed to set system tables GUCs.")
            self.__systable_mods_set = True

    def create_catdml(self):
        if not self.__catdml_created:
            self.db.query("BEGIN")
            self.db.query("""
            create or replace function pg_catalog.catDML(stmt text)
            returns int as $$
            declare
              contentid integer;
            begin
              SELECT INTO contentid current_setting('gp_contentid');
              IF contentid = -1 THEN
                PERFORM pg_catalog.catDML(stmt) FROM gp_dist_random('gp_id');
              END IF;

              execute stmt;
              return 1;
            end;
            $$ language 'plpgsql';
            """)
            self.db.query("COMMIT")
            self.__catdml_created = True

    def find_orphaned_visimaps(self):
        """Returns a list of relnames, one for each orphaned visimap.

        """
        query = (
            """SELECT relname FROM pg_catalog.pg_class WHERE relname LIKE
            'pg_aovisimap%' AND relkind = 'm' AND relnamespace=6104 AND
            oid NOT IN (SELECT visimaprelid FROM pg_catalog.pg_appendonly);
            """)
        logger.debug(query)
        visimaps = self.db.query(query).getresult()
        return map(lambda x: x[0], visimaps)

    def drop_orphaned_visimaps(self, visimaps):
        """Dropping a visimap relation is tricky because it is not a regular
        relation (relkind='m').  We make it a regular relation
        (relkind='r', relnamespace=public) here and then execute DROP
        TABLE on it.  visimaps is a list of visimap relnames.

        """
        self.allowSystableMods()
        self.create_catdml()
        self.db.query("BEGIN")
        for relname in visimaps:
            query = (
                """SELECT catDML('update pg_catalog.pg_class set relkind=''r'',
                relnamespace=%s where oid = ' || oid) FROM pg_catalog.pg_class
                WHERE relname='%s';""" % (self.public_schema_oid, relname))
            logger.debug(query)
            self.db.query(query)
            query = "DROP TABLE public.%s" % relname
            logger.debug(query)
            self.db.query(query)
        self.db.query("COMMIT")
        count = len(self.find_orphaned_visimaps())
        if count > 0:
            self.cleanup()
            raise GPDBException("Found %d orphaned visimaps even after drop." %
                                count)

    def find_troubled_ao_tables(self):
        """Returns a list of tuples (relname, visimapname, relid,
        visimaprelid) for each appendonly table (relid) which existed
        in 4.2.x prior to upgrade, visimap dependency for which needs
        to be fixed.

        """
        query = (
            """SELECT relid::regclass, visimaprelid::regclass, relid,
            visimaprelid FROM pg_catalog.pg_appendonly WHERE visimaprelid NOT IN
            (SELECT objid FROM pg_catalog.pg_depend, pg_catalog.pg_appendonly
            WHERE pg_depend.refobjid = pg_appendonly.relid);""")
        logger.debug(query)
        aotables = self.db.query(query).getresult()
        # Convert list of tuples of type (str, str, str) to (str, int, int).
        return map(lambda x: (x[0], x[1], int(x[2]), int(x[3])), aotables)

    def add_dependency(self, aotables):
        """Record dependency in pg_depend as visimapOid --> parent appendonly
        relation (relid).  aotables is a tuple returned by
        find_troubled_ao_tables().

        """
        self.allowSystableMods()
        self.create_catdml()
        self.db.query("BEGIN")
        for row in aotables:
            if len(row) != 4:
                self.cleanup()
                raise GPDBException("Invalid input: %s" % str(row))
            relid, visimapOid = row[2:]
            query = (
                """select catDML('insert into pg_catalog.pg_depend values
                (1259, ' || %d || ', 0, 1259, ' || %d || ', 0, ''i'')')""" %
                (visimapOid, relid))
            logger.debug(query)
            self.db.query(query)
        self.db.query("COMMIT")

    def find_spurious_toasts(self):
        """Retruns a tuple (toastoid, toastrelname, visimapoid) for each
        visimap relation that has a corresponding toast.

        """
        query = (
            """SELECT reltoastrelid, reltoastrelid::regclass, oid FROM
            pg_catalog.pg_class WHERE reltoastrelid > 0 AND
            oid IN (SELECT visimaprelid FROM pg_catalog.pg_appendonly);
            """)
        logger.debug(query)
        toasts = self.db.query(query).getresult()
        # Convert [("oid", "pg_toast.pg_toast_*", "oid"), ...] -->
        # [(oid, "pg_toast_*", oid), ...]
        return map(lambda x: (int(x[0]), x[1].split(".")[1], int(x[2])),
                   toasts)

    def drop_spurious_toasts(self, toasts):
        """toasts is a list of tuples of the form (toastoid, toastname,
        visimapoid)

        """
        self.allowSystableMods()
        self.create_catdml()
        self.db.query("BEGIN")
        for toastoid, toastname, visimapoid in toasts:
            # Delete dependency of toast relation with visimap relation.
            query = (
                """SELECT catDML('DELETE FROM pg_catalog.pg_depend WHERE
                refclassid=1259 AND classid=1259 AND objid=%d AND refobjid=%d')
                """ % (toastoid, visimapoid))
            logger.debug(query)
            self.db.query(query)
            # Make toast relation a normal user table, so that DROP
            # TABLE can drop it.
            query = (
                """ SELECT catDML('UPDATE pg_catalog.pg_class SET relkind=''r'',
                relnamespace=%s WHERE oid=%d')""" % (self.public_schema_oid, toastoid))
            logger.debug(query)
            self.db.query(query)
            query = "DROP TABLE public.%s" % toastname
            logger.debug(query)
            self.db.query(query)
            query = (
                """SELECT catDML('UPDATE pg_catalog.pg_class SET reltoastrelid=0
                WHERE oid=%d')""" % visimapoid)
            logger.debug(query)
            self.db.query(query)
        self.db.query("COMMIT")

    def cleanup(self):
        self.db.query("BEGIN")
        self.db.query("DROP FUNCTION IF EXISTS pg_catalog.catDML(text)")
        if self.public_schema_is_created:
            self.db.query("DROP SCHEMA public")
        self.db.query("COMMIT")
        self.db.close()
        self.__systable_mods_set = False
        self.public_schema_is_created = False

def parseargs(argv):
    parser = OptionParser(add_help_option=False)
    parser.add_option("-r", "--report", dest="report", action="store_true",
                      default=False,
                      help="Report inconsistencies without making any change")
    # Pygresql defaults to localhost, 5432, $USER for the following
    # three options, respectively.  We therefore do not need any
    # defaults.
    parser.add_option("-h", "--host", dest="host", action="store",
                      type="string", help="GPDB master hostname/IP address")
    parser.add_option("-p", "--port", dest="port", action="store", type="int",
                      help="GPDB master port")
    parser.add_option("-u", "--user", dest="user", action="store",
                      type="string",
                      help="Username to connect to GPDB.  Must be superuser")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                      default=False,
                      help="Verbose logging/output, include table names.")
    parser.add_option("--help", dest="help", action="store_true",
                      default=False, help="Show this help message and exit")
    (options, args) = parser.parse_args(argv)
    # If no args are specified, don't assume anything, print help and exit.
    if options.help or len(argv) == 1:
        print parser.format_help().strip()
        sys.exit(0)
    if options.user is not None:
        options.passwd = getpass.getpass()
    else:
        options.passwd = None
    return options

def get_dbs(host, port, user, passwd):
    gpdb = GPDB("postgres", host, port, user, passwd)
    tuples = gpdb.db.query(
        "SELECT datname FROM pg_database WHERE datallowconn='t'").getresult()
    gpdb.cleanup()
    # Convert [(relname,), (relname,), ...] to [relname, relname, ...]
    return map(lambda x: x[0], tuples)

def rectify_db(dbname, host, port, user, passwd, dryrun=False):
    """ Fixes/reports inconsistencies in one database.  Returns True if
    inconsistencies found or fixes applied.  No modifications are
    performed if dryrun=True. """
    fixed_or_found = False
    gpdb = GPDB(dbname, host, port, user, passwd)
    visimaps = gpdb.find_orphaned_visimaps()
    if len(visimaps) > 0:
        fixed_or_found = True
        logger.info("Found %d orphaned visimap tables." % len(visimaps))
        if dryrun:
            for relname in visimaps:
                logger.debug("    %s" % relname)
        else:
            gpdb.drop_orphaned_visimaps(visimaps)
            logger.info("Cleaned up orphaned visimap tables.")
    aotables = gpdb.find_troubled_ao_tables()
    if len(aotables) > 0:
        logger.info(
            "Found %d appendonly tables whose dependency with visimaps" %
            len(aotables) + " needs to be rectified.")
        fixed_or_found = True
        if dryrun:
            for row in aotables:
                logger.debug("    %s --> %s" % (row[1], row[0]))
        else:
            gpdb.add_dependency(aotables)
            logger.info("Visimap dependencies rectified.")
    toasts = gpdb.find_spurious_toasts()
    if len(toasts) > 0:
        logger.info("Found %d spurious toast relations." % len(toasts))
        fixed_or_found = True
        if dryrun:
            for row in toasts:
                logger.debug("    %s" % row[1])
        else:
            gpdb.drop_spurious_toasts(toasts)
            logger.info("Dropped spurious toast relations.")
    gpdb.cleanup()
    return fixed_or_found

if __name__ == "__main__":
    options = parseargs(sys.argv)
    try:
        logger = gplog.setup_tool_logging(
            EXEC_NAME, socket.gethostname().split('.')[0], getpass.getuser())
        gplog._SOUT_HANDLER.addFilter(ExceptionFilter())
        if options.verbose:
            gplog.enable_verbose_logging()
        for db in get_dbs(options.host, options.port, options.user,
                          options.passwd):
            print
            logger.info("Database %s:" % db)
            fixed = rectify_db(db, options.host, options.port, options.user,
                               options.passwd, options.report)
            if not fixed:
                logger.info("No inconsistencies found in %s." % db)
            elif not options.report:
                logger.info("Upgrade fix applied to %s." % db)
    except (pg.InternalError, GPDBException), e:
        logger.exception(e)
        sys.exit(e)
