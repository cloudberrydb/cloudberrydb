#!/usr/bin/env python
import subprocess
import tempfile
import re
import os
import sys

files = {}
files["33"] = ["loadcat33.source.in",
               "upg2_pg_class_toadd.data.in",
               "upg2_pg_attribute_toadd.data.in",
               "upg2_pg_type_toadd.data.in",
               "upg2_pg_index_toadd.data.in",
               "upg2_pg_proc_toadd.data",
               "upg2_pg_namespace_toadd.data"]

def dbname_oid(db):
    # XXX: hack for now
    if db == "template0":
        return 10828
    elif db == "template1":
        return 1
    elif db == "postgres":
        return 10829
    else:
        return None

def get_new_files(procfile):
    out = []
    f = open(procfile, 'r')
    for line in f:
        # OID is first col and is equal to the relfilenode at this stage
        pos = line.find('|')
        if not pos:
            raise Exception("can't find |")
        oid = int(line[:pos])
        out.append(oid)
    f.close()
    return out

def touch_files(base, files):
    for f in files:
        path = os.path.join(base, f)
        fp = open(path, 'w')
        fp.close()

def transform(path, dbs, tfile):
    for db in dbs:
    #    try:
            transform_db(path, db, tfile)
     #   except Exception, e:
      #      raise Exception("cannot upgrade %s: %s" % (db, e))

def transform_db(path, db, tfile):
    # get upgrade files
    home  = os.environ.get('HOME')
    lpath = os.environ.get('LD_LIBRARY_PATH')
    dypath = os.environ.get('DYLD_LIBRARY_PATH')

    # Add $GPHOME/bin to the path for this environment
    gphome = '/Users/swm/greenplum-db-devel/'
    envpath = '/Users/swm/greenplum-db-devel/bin:/Users/swm/greenplum-db-devel/bin/ext/python/bin:%s' % os.environ.get('PATH')

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
    env['PATH']    = envpath
    env['LD_LIBRARY_PATH'] = lpath
    env['DYLD_LIBRARY_PATH'] = dypath
    masterdir = os.environ.get('MASTER_DATA_DIRECTORY')
    if masterdir:
        env['MASTER_DATA_DIRECTORY'] = masterdir
        env['PGPORT'] = os.environ.get('PGPORT')

    print "loading " + db
    load = open(tfile, 'r')

    # create files to back new relations
    new_files = get_new_files(os.path.join([dirname(tfile),
                                           'upg2_pg_class_todata.data']))
    touch_files(os.path.join([path, 'base', dbname_oid(db)], new_files)


    # run postgres in single user mode, feed in scripts
    cmd = ' '.join(['/Users/swm/greenplum-db-devel/bin/postgres', '--single', '-O', '-P', 
                    '-c', 'gp_session_role=utility',
                    '-c', 'exit_on_error=true',
                    '-D', path, db])
    print cmd
    p = subprocess.Popen(cmd, shell = True,
                          close_fds = True,
                          stdin = load,
                          #stdout = subprocess.PIPE,
                          stdout = sys.stdout,
                          #stderr = subprocess.PIPE,
                          stderr = sys.stderr,
                          env=env)
    result = p.communicate()

    if p.returncode == 0:
        print "all good"
    else:
        print "here"
        raise Exception("oh no")


def make_files(files, datadir, user):
    """
    Take the set of .in files and apply translation to the local
    environment
    """

    tmpdir = tempfile.mkdtemp()
    print "created dir " + tmpdir
    out = []

    transforms = [['@gpupgradeschemaname@', 'pg_catalog'],
                  ['@gpupgradedatadir@', tmpdir],
                  ['@gpcurusername@', user]
                 ]
    for file in files:
        if file[-3:] == ".in":
            outfilename = os.path.join(tmpdir, file[:-3])
        else:
            outfilename = os.path.join(tmpdir, file)

        outf = open(outfilename, 'w')
        inf = open(os.path.join(datadir, file), 'r')
        for line in inf:
            #print "--" + line
            for t in transforms:
                line = re.sub(t[0], t[1], line)
                #print "@" + line
            outf.write(line)
        outf.close()
        inf.close()
        out.append(outfilename)
        print "created %s" % outfilename
    return out

if __name__ == '__main__':
    fs = make_files(files["33"],
    '/Users/swm/greenplum-db-devel/share/postgresql/upgrade/',
    'swm')

    dbs = ['postgres', 'template0', 'template1']
    paths = ['/Users/swm/greenplum-db-devel/upg/upgradetest-1',
             '/Users/swm/greenplum-db-devel/upg/upgradetest0',
             '/Users/swm/greenplum-db-devel/upg/upgradetest1']

    for p in paths:
        transform(p,
                  dbs,
                  fs[0] # loadcat33 must be first
                 )
