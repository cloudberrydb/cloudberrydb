# -*- coding: utf-8 -*-

import pg8000
import os
import shutil
import subprocess

def do_copy(src, dst, f):
    if os.path.isdir(os.path.join(src, f)):
        shutil.copytree(os.path.join(src, f), os.path.join(dst, f))
    else:
        shutil.copy2(os.path.join(src, f), os.path.join(dst, f))

        # for this is a file, copy all segments
        if os.path.isfile(os.path.join(src, f)):
            for i in range(1, 65000):
                i = str(i)
                if os.path.exists(os.path.join(src, f, i)):
                    shutil.copy2(os.path.join(src, f, i),
                                 os.path.join(dst, f, i))
                else:
                    break


# copy files from src to dst
# create all necessary directories then copy files
# need to make sure we copy all segment files, so N and N.1, N.2, ...
def buildskel(src, dst, files, meta):
    # build list for directories to create
    dirs = []

    # run through meta first, they're all directories pretty much
    for d in meta:
        path = os.path.join(src, d)
#        print "testing " + path
        if os.path.isdir(path):
            # will be created in do_copy()
            continue
        else:
            d = os.path.dirname(d)
            path = os.path.join(src, d)
            if not os.path.isdir(path):
                raise Exception("%s should be a directory!" % path)
        
        # only add this if the path != src
        if not os.path.samefile(src, path):
#            print "path = %s, src = %s" % (path, src)
#            print "adding %s" % d
            dirs.append(d)

    for dbname, fs in files.iteritems():
        for f in fs:
            if os.path.isdir(os.path.join(src, f)):
                # will be created in do_copy()
                continue
            if os.path.isfile(os.path.join(src, f)):
                f = os.path.dirname(f)
#            print "dbname = %s, f = %s" % (dbname, f)
            path = os.path.join(src, f)
            if not os.path.isdir(path):
                raise Exception("%s should be a directory!" % path)

            # only add this if the path != src
            if not os.path.samefile(src, path):
#                print "path = %s, src = %s" % (path, src)
                dirs.append(f)

    dirs = list(set(dirs)) # make unique
    print dirs

    # make sure dst exists
    if not os.path.exists(dst):
        os.mkdir(dst, 0700)

    for d in dirs:
        # mkdir -p
        print "making dir %s" % d
        os.makedirs(os.path.join(dst, d), 0700)

    print "copying files"

    for dbname, fs in files.iteritems():
        for f in fs:
            do_copy(src, dst, f)

    for f in meta:
        do_copy(src, dst, f)

# for each database connection handle, get the set of catalog relfilenodes
# necessary for building a skeleton for upgrade.
# dbs is a map of dbname => pg8000 connection handle
# assumes that the database is locked down so that only we can connect
def dbgetcatfiles(dbs):
    files = {}
    for name, conn in dbs.iteritems():
        first = True
        try:
            sql = str('''select relfilenode, d.oid as datoid
                from pg_class c, pg_database d 
                where relnamespace = 11 and d.datname = '%s'
                and c.relkind in ('r', 't', 'i') and 
                relisshared = 'f';''' % name)
            conn.execute(sql)
        except pg8000.errors.ProgrammingError, p:
            # XXX: handle better
            raise p

        f = []
        for row in conn.iterate_dict():
            if first:
                first = False
                file = os.path.join('base', str(row['datoid']),
                                    'PG_VERSION')
                f.append(file)
            file = os.path.join('base', str(row['datoid']),
                               str(row['relfilenode']))
            f.append(file)
        files[name] = f

    # add template0
    conn.execute("select oid from pg_database where datname = 'template0'")
    if conn.row_count:
        files['template0'] = [os.path.join('base', 
                                           str(conn.iterate_tuple().next()[0]))]

    # meta data directories, like commit log
    meta = ['global', 'pg_clog', 'pg_distributedlog',
            'pg_distributedxidmap', 'pg_multixact', 'pg_subtrans',
            'pg_twophase', 'pg_utilitymodedtmredo', 'pg_xlog',
            'postgresql.conf', 'pg_hba.conf', 'pg_ident.conf', 'PG_VERSION']
    return [files, meta]


if __name__ == '__main__':
    postgres = pg8000.Connection(user='swm', database='postgres', port=12000,
                                 host='localhost',
                                 options='-c gp_session_role=utility')
    postgres.execute('''select datname, datallowconn from pg_database where
            datname not in('template0', 'postgres');''')
    dbs = {}
    dbs['postgres'] = postgres
    for row in postgres.iterate_dict():
        if not row['datallowconn']:
            raise "database %s has datallowconn set to false" % row['datname']
        dbname = row['datname']
        conn = pg8000.Connection(user='swm', database=dbname, port=12000,
                                 host='localhost',
                                 options='-c gp_session_role=utility')
        dbs[dbname] = conn

    (files, meta) = dbgetcatfiles(dbs)

    # close connections
    for dbname, conn in dbs.iteritems():
        conn.close()

    # XXX: stop the system, do better in migrator
    home  = os.environ.get('HOME')
    lpath = os.environ.get('LD_LIBRARY_PATH')
    dypath = os.environ.get('DYLD_LIBRARY_PATH')

    # Add $GPHOME/bin to the path for this environment
    gphome = '/Users/swm/greenplum-db-devel/'
    path = '/Users/swm/greenplum-db-devel/bin:/Users/swm/greenplum-db-devel/bin/ext/python/bin:%s' % os.environ.get('PATH')

    if lpath:
        lpath = '%s/lib:%s/ext/python/lib:%s' % (gphome, gphome, lpath)
    else:
        lpath = '%s/lib:%s/ext/python/lib' % (gphome, gphome)
    if dypath:
        dypath = '%s/lib:%s/ext/python/lib:%s' % (gphome, gphome, dypath)
    else:
        dypath = '%s/lib:%s/ext/python/lib' % (gphome, gphome)

    env = {}
    env['HOME']    = home
    env['USER']    = os.environ.get('USER')
    env['LOGNAME'] = os.environ.get('LOGNAME')
    env['GPHOME']  = gphome
    env['PATH']    = path
    env['LD_LIBRARY_PATH'] = lpath
    env['DYLD_LIBRARY_PATH'] = dypath
    masterdir = os.environ.get('MASTER_DATA_DIRECTORY')
    if masterdir:
        env['MASTER_DATA_DIRECTORY'] = masterdir
        env['PGPORT'] = os.environ.get('PGPORT')

    p = subprocess.Popen("gpstop -a", env=env, shell=True)
    p.communicate()
    r = p.returncode
    if r != 0:
        os.system("ps auxww|grep postgres")
        raise Exception("couldn't stop system")

    buildskel('/Users/swm/greenplum-db-devel/cdb-data/testdb-1',
              '/Users/swm/greenplum-db-devel/upg/upgradetest-1',
              files, meta)
    buildskel('/Users/swm/greenplum-db-devel/cdb-data/testdb1',
              '/Users/swm/greenplum-db-devel/upg/upgradetest1',
              files, meta)
    buildskel('/Users/swm/greenplum-db-devel/cdb-data/testdb0',
              '/Users/swm/greenplum-db-devel/upg/upgradetest0',
              files, meta)
