#!/usr/bin/env python
'''
gpsys.py -- print system properties

Usage: gpsys.py [-p] [-t]

	-p  : print system properties in Python pickled format
        -t  : print system properties in Text (default).
'''

import os, platform, sys, getopt, pickle
from datetime import datetime

opt = {}
opt['-p'] = False
GPHOME=os.getenv('__GPHOME')
if not GPHOME:
    GPHOME = os.getenv('GPHOME')

def makeCommand(cmd):
    return ('__GPHOME=%s && GPHOME=$__GPHOME && export GPHOME '
            '&& PATH=$GPHOME/bin:$PATH && export PATH '
            '&& LD_LIBRARY_PATH=$GPHOME/lib:$LD_LIBRARY_PATH && export LD_LIBRARY_PATH '
            '&& %s'
            % (GPHOME, cmd))

################
def usage(exitarg):
    print __doc__
    sys.exit(exitarg)


def parseCommandLine():
    global opt
    try:
        (options, args) = getopt.getopt(sys.argv[1:], 'pt')
    except Exception, e:
        usage('Error: ' + str(e))

    for (switch, val) in options:
        if switch == '-p':	opt['-p'] = True
        elif switch == '-t':    opt['-p'] = False

def run(cmd):
    f = None
    ok = False
    out = []
    try:
        f = os.popen(cmd)
        for line in f:
            out.append(line)
        ok = not f.close()
    finally:
        if f: f.close()

    return (ok, out)

def add(res, prefix, lines):
    for line in lines:
        x = line.split(' ', 1)
        if len(x) == 2:
            x[0] = x[0].strip()
            x[1] = x[1].strip()
            if (x[0] and x[1]):
                if x[0][-1] == ':':
                    x[0] = x[0][:-1]
                elif x[1][0] == ':':
                    x[1] = x[1][1:]
                elif x[0][-1] == '=':
                    x[0] = x[0][:-1]
                elif x[1][0] == '=':
                    x[1] = x[1][1:]

                if x[0]:
                    res[prefix + x[0].lower().strip()] = x[1].strip()
    return res


def do_gppath(res):
    out = os.getenv("GPHOME")
    if out is None:
	out = ''
    res['env.GPHOME'] = out.strip()
    out = os.getenv('__GPHOME')
    if out is None:
        out = ''
    res['env.__GPHOME'] = out.strip()
    return True


def do_postgres_md5(res):
    cmd = makeCommand("cat $__GPHOME/bin/postgres | "
                      "python -c 'import md5, sys; m = md5.new(); m.update(sys.stdin.read()); print m.hexdigest()'")
    (ok, out) = run(cmd)
    if ok:
        for line in out:
            if len(line) == 33:
                res['postgres.md5'] = line.lower().strip()
                return True
    return False


def do_postgres_version(res):
    cmd = makeCommand("$__GPHOME/bin/postgres --version")
    (ok, out) = run(cmd)
    if ok and len(out) == 1: 
        res['postgres.version'] = out[0].strip()
        return True
    return False
            


def do_sysctl(res):
    (ok, out) = run('export PATH="/sbin:/usr/sbin:$PATH" && sysctl -a 2> /dev/null')
    if ok:
        add(res, 'sysctl.', out)
    return ok


def do_ulimit(res):
    (ok, out) = run('ulimit -u && ulimit -n')
    if ok:
        res['ulimit.nproc'] = out[0].strip()
        res['ulimit.nofile'] = out[1].strip()
    return ok


def do_sync(res):
    res['sync.time'] = datetime.today()

def do_platform(res):
    res['platform.platform'] = platform.platform()
    uname = platform.uname()
    res['platform.system'] = uname[0].lower()
    res['platform.node'] = uname[1]
    res['platform.release'] = uname[2]
    res['platform.version'] = uname[3]
    res['platform.machine'] = uname[4]
    res['platform.processor'] = uname[5]
    s = res['platform.system']
    mem = 0
    if (s.find('sunos') >= 0): 
        (ok, out) = run('''sh -c "/usr/sbin/prtconf | awk '/^Memory/{print}'"''')
        if ok: 
            list = out[0].strip().split(' ')
            val = int(list[2])
            factor = list[3]
            if factor == 'Megabytes':
                mem = val * 1024 * 1024
    elif (s.find('linux') >= 0):
	ok, out = run("sh -c 'cat /proc/meminfo | grep MemTotal'")
        if ok:
            list = out[0].strip().split(' ')
            val = int(list[len(list) - 2])
            factor = list[len(list) - 1]
            if factor == 'kB': 
                mem = val * 1024
    elif (s.find('darwin') >= 0):
	(ok, out) = run("/usr/sbin/sysctl hw.physmem")
        if ok:
            list = out[0].strip().split(' ')
            mem = int(list[1])

    res['platform.memory'] = mem
    return True

def do_python(res):
    version = sys.version_info
    res['python.version'] = '%s.%s.%s' % version[0:3]

def do_system(res):
    SYSTEM_KEYS = ('rlim_fd_max', 'rlim_fd_cur', 'shmsys:shminfo_shmmax', 'semsys:seminfo_semmni')
    f = open('/etc/system', 'r')
    content = f.read()
    f.close()
    
    p = []
    lines = content.splitlines()
    for line in lines:
        line = line.strip()
        if line.startswith('set'):
            for key in SYSTEM_KEYS:
                if line.find(key) != -1:
		    res['system.%s' % key] = line[3:].split('=')[-1].strip()
                    break

    return True

def do_meminfo(res):
    if not os.path.exists('/proc/meminfo'):
        return False

    f = None
    try:
        f = open('/proc/meminfo', 'r')
        list = []
        for line in f:
            list.append(line)
        add(res, '/proc/meminfo', list)
        return True
    finally:
        if f: f.close()

def do_ndd(res):
    if not os.path.exists('/usr/sbin/ndd'):
        return False

    list = ('tcp_conn_req_max_q', 'tcp_conn_req_max_q0', 'tcp_largest_anon_port', \
                'tcp_smallest_anon_port', 'tcp_time_wait_interval')
    for key in list:
        (ok, out) = run('/usr/sbin/ndd /dev/tcp ' + key)
        if ok and len(out) == 1:
            res['ndd.' + key] = out[0].strip()

def do_solaris(res):
    if not os.path.exists('/etc/release'):
        return False
    f = open('/etc/release')
    lines = f.readlines();
    f.close();
    for i in lines:
	i = i.strip()
        if i.startswith('Solaris 10'):
            res['solaris.release'] = i
    f = os.popen("ls -1 /var/sadm/patch ", "r")
    files = f.readlines()
    f.close()
    res['solaris.patch_file'] = ' '.join(files)
    f = os.popen("/bin/showrev -p", "r")
    lines = f.readlines();
    f.close()
    patch = []
    for i in lines:
	i = i.strip() 
	i = i.split()
	if len(i) > 2:
	    patch.append(i[1]);
    res['solaris.patch'] = ' '.join(patch)

def do_zfs(res):
    (ok, out) = run('/sbin/zpool list -H')
    if ok and len(out) == 1:
        r = out[0].split()
        if len(r) == 7:
            res['zfs.health'] = r[5].strip()
            (ok, out) = run('/sbin/zfs get -H checksum %s' % r[0])
            if ok and len(out) == 1:
                r = out[0].split()
                if len(r) == 4:
                    res['zfs.checksum'] = r[2].strip()


parseCommandLine()
res = {}

do_gppath(res)
do_postgres_md5(res)
do_postgres_version(res)
do_python(res)
do_platform(res)
do_sync(res)
system = res['platform.system']
if system == 'sunos':
    do_zfs(res)
    do_system(res)
    do_ndd(res)
    do_solaris(res)
elif system == 'linux':
    do_sysctl(res)
    do_ulimit(res)
    do_meminfo(res)
elif system == 'darwin': 
    do_sysctl(res)
    do_ulimit(res)


if opt['-p']:
    print "BEGINDUMP"
    print pickle.dumps(res)
    print "ENDDUMP"
else:
    for i in res:
        print i, ' | ', res[i]

