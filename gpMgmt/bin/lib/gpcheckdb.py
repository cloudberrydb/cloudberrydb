#!/usr/bin/env python 

'''
gpcheckdb - checks db for required upkeep actions

Usage: gpcheckdb [-U uname] [-h host] [-p port] [-d dbname]

	-U: database user name (PGUSER)
        -h: database server host (PGHOST)
	-p: database server port (PGPORT)
        -d: database name (PGDATABASE)

        -v: verbose
        -V: very verbose

'''

import os, sys
progname = os.path.split(sys.argv[0])[-1]
if sys.version_info < (2,5,0):
    sys.exit(
'''Error %s is supported on Python version 2.5 or greater
Please upgrade python installed on this machine.''' % progname)

import subprocess, time, datetime, threading, Queue, pickle, random


############

class __globals__:
    opt = {}
    for o in 'vV': opt['-' + o] = False
    opt['-U'] = os.getenv('PGUSER') or ''
    opt['-h'] = os.getenv('PGHOST') or ''
    opt['-p'] = os.getenv('PGPORT') or ''
    opt['-d'] = os.getenv('PGDATABASE') or ''


GV = __globals__()

############
def usage(exitarg):
    print __doc__
    sys.exit(exitarg)

############
def humantime(td):
    d = td.days > 0 and td.days or 0
    h = int(td.seconds / 60 / 60)
    m = int(td.seconds / 60) % 60
    s = td.seconds % 60
    ret = ''
    if d: ret = ret + '%dD ' % d
    if h: ret = ret + '%dh ' % h
    ret = ret + ('%dm %ds' % (m, s))
    return ret

############
def tstamp():
    return datetime.datetime.now().strftime('[%Y-%m-%d %H:%M:%S]')


############
def msg(s):
    print '%s %s' % (tstamp(), s)

def vmsg(s):
    if GV.opt['-v']: msg(s)

def vvmsg(s):
    if GV.opt['-V']: msg(s)

############
def die(s):
    sys.exit('%s ERROR: %s' % (tstamp(), s))


############
def confirm(s):
    if not GV.opt['-f'] and sys.stdin.isatty():
        ok = raw_input('%s\n ... proceed (y/n)? ' % s)
        print
        ok = ok.strip().lower()
        return ok and ok[0] == 'y'
    return True

############
def parseCommandLine():
    import getopt
    try:
        (options, args) = getopt.getopt(sys.argv[1:], '?VvU:h:p:d:')
    except Exception, e:
        usage('Error: ' + str(e))

    for (switch, val) in options:
        if switch == '-?': usage(0)
        elif switch[1] in 'Vv': GV.opt[switch] = True
        elif switch[1] in 'Uhpd': GV.opt[switch] = val

    if not GV.opt['-d']:
        usage('Error: please specify -d database')

    if not GV.opt['-U']:
        usage('Error: please specify -U user')
    
############
def run(cmd):
    vvmsg(cmd)
    p = os.popen(cmd)
    out = p.readlines()
    if GV.opt['-V']:
        for line in out: vvmsg(line[:-1])
    rc = p.close()
    return (rc, out)

#####################################################
def psql_open(sql, echo=False, quiet=True):
    sql = sql.strip()
    if echo: msg("SQL: " + sql)
    cmd = ['psql']
    if quiet: cmd.append('-q')
    cmd.append('-At')
    cmd.append('-c')
    cmd.append(sql)
    stime = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    p.x_echo = echo
    p.x_quiet = quiet
    p.x_stime = stime
    p.x_sql = sql
    return p


def psql_wait(p):
    out = p.stdout.readlines()
    rc = p.wait()
    etime = time.time()
    if not p.x_quiet:
        for i in out: print i,
    if rc:
        die('PSQL ERROR\nSQL: ' + p.x_sql)

    if p.x_echo:
        msg("ELAPSED: " + str(etime - p.x_stime))

    return out

def psql(sql, echo=False, quiet=True):
    p = psql_open(sql, echo, quiet)
    return psql_wait(p)

############
def pmap(func, jlist, numThreads = 16):
    if (numThreads > len(jlist)): 
        numThreads = len(jlist)

    inq = Queue.Queue(len(jlist))
    for i in jlist: inq.put(i)
    
    outq = Queue.Queue(len(jlist))
    def work():
        try:
            while True: 
                outq.put((None, func(inq.get_nowait())))
        except Queue.Empty: pass
        except:
            outq.put( (sys.exc_info(), None) )
            # drain 
            try:
                while True: inq.get_nowait()
            except Queue.Empty: pass

    thread = [threading.Thread(target=work) for i in xrange(numThreads)]
    for t in thread: t.start()
    for t in thread: t.join()
    
    ret = []
    try: 
        while True: 
            (ex, result) = outq.get_nowait()
            if ex: 
                raise ex
            ret.append(result)
    except Queue.Empty: pass
    return ret

############
def chk_not_analyzed():
    print '-----------------------'
    print 'LOOKING FOR TABLES NOT ANALYZED'
    out = psql('''
SELECT '    * ' || nspname || '.' || relname 
  from pg_class, pg_namespace 
 where reltuples=0 and relpages=0 and relnamespace=pg_namespace.oid 
       and nspname not in ('information_schema','pg_aoseg',
                           'pg_bitmapindex', 'pg_catalog', 'pg_toast') 
       and relkind='r'
 order by 1
''')
    print '    %d object(s) found' % len(out)
    print "".join(out)


############
def chk_users_without_resource_queues():
    print '-----------------------'
    print 'LOOKING FOR USERS WITHOUT RESOURCE QUEUES'
    out = psql('''
SELECT '    * ' || rolname
  from pg_roles 
 where rolresqueue is null and rolsuper='f'
 order by 1
''')
    print '    %d object(s) found' % len(out)
    print "".join(out)


############
def chk_tables_with_big_skew():
    print '-----------------------'
    print 'LOOKING FOR TABLES WITH BIG SKEW'
    out = psql('''
select ' '||max(c)||' ' ||min(c)||' '|| avg(c)||' '|| stddev(c)||' '|| (max(c) - min(c))/max(c) as p_diff_max_min
from (
     select case when c is null then 0 else c end, gp_segment_id_present     
       from (select generate_series(0,79) as gp_segment_id_present ) t1 
            left outer join 
            (select count(*) as c, gp_segment_id from :table group by 2) t2 
            on t1.gp_segment_id_present =t2.gp_segment_id    
) as data     
''')


############
def chk_guc():
    print '-----------------------'
    print 'CHECKING GUCS'
    out = psql('''
SELECT current_setting('lc_collate'),
       current_setting('lc_monetary'),
       current_setting('lc_numeric'),
       current_setting('max_connections'),
       current_setting('gp_fault_action'),
       current_setting('work_mem')
''')
    (lc_collate, lc_monetary, lc_numeric, max_connections,
     gp_fault_action, work_mem)  = out[0].strip().split('|')
    print '    lc_collate      =', lc_collate
    print '    lc_monetary     =', lc_monetary
    print '    lc_numeric      =', lc_numeric
    print '    max_connections =', max_connections
    print '    gp_fault_action =', gp_fault_action
    print '    work_mem        =', work_mem
    

############
def main():
    parseCommandLine()
    # set up env for psql
    os.putenv("PGOPTIONS", '-c gp_session_role=utility')
    os.putenv("PGDATABASE", GV.opt['-d'])
    os.putenv("PGHOST", GV.opt['-h'])
    os.putenv("PGPORT", GV.opt['-p'])
    os.putenv("PGUSER", GV.opt['-U'])

    # check for tables not analyzed
    chk_not_analyzed()

    # check for tables with significant skew
    #chk_tables_with_big_skew()

    # check for users not associated with any resource queues
    chk_users_without_resource_queues()

    # check for a few guc settings (e.g., gp_vmem_protect_limit)
    chk_guc()


    

if __name__ == '__main__':
    main()
