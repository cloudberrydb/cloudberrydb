#!/usr/bin/env python 

'''
gpsnap -- snapshot a gpdb array

Usage: gpsnap [-Vvlfz?] [-u user] {-crdiI} snapshot_name

	-c: create a snapshot
        -r: restore a snapshot
        -d: delete a snapshot
        -i: show information on snapshot
        -I: show detailed info on snapshot

        -z: use zfs snapshot mechanism
        -l: list snapshots
        -v: verbose
        -V: very verbose
        -f: force
        -?: print help

'''

import os, sys
progname = os.path.split(sys.argv[0])[-1]
if sys.version_info < (2,5,0):
    sys.exit(
'''Error %s is supported on Python version 2.5 or greater
Please upgrade python installed on this machine.''' % progname)

import subprocess, time, datetime, threading, Queue, random, pickle


############
MASTER_DATA_DIRECTORY = os.getenv('MASTER_DATA_DIRECTORY')
if not MASTER_DATA_DIRECTORY:
    sys.exit('MASTER_DATA_DIRECTORY env not defined')



# we always connect to localhost for snapshot ... must run on master machine
os.putenv('PGHOST', '')
os.putenv("PGOPTIONS", '-c gp_session_role=utility')
os.putenv('PGDATABASE', 'template1')

class __globals__:
    opt = {}
    for o in 'vVcrdiIflzu': opt['-' + o] = False
    opt['-u'] = ''
    snapname = ''


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
def strip_trailing_slash(dir):
	return dir.rstrip('/') if dir else None

############
def ssh(hostname, cmd, input=''):
    proxy = ['ssh', '-o', 'BatchMode yes', '-o', 'StrictHostKeyChecking no', hostname,
             '''bash -c 'exec python -c "import os,sys,pickle,subprocess; (prog, cmd, input) = pickle.load(sys.stdin); exec prog" ' ''']
    prog = """
p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
p.stdin.write(input)
p.stdin.close()
sys.stdout.write(p.stdout.read())
sys.exit(p.wait())"""
    vvmsg('  - ssh ' + hostname + ' ' + str(cmd))
    p = subprocess.Popen(proxy, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    if type(cmd) == type(''):
        cmd = ['bash', '-c', cmd]
    pickle.dump( (prog, cmd, input), p.stdin )
    p.stdin.close()
    out = p.stdout.read()
    rc = p.wait()
    return (rc, out)
    
    

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
        (options, args) = getopt.getopt(sys.argv[1:], '?VvlcrdiIfzu:')
    except Exception, e:
        usage('Error: ' + str(e))

    for (switch, val) in options:
        if switch == '-?': usage(0)
        elif switch[1] in 'VvlcrdiIfz': GV.opt[switch] = True
        elif switch == '-u': GV.opt[switch] = val

    if GV.opt['-V']: GV.opt['-v'] = True
        
    if 1 == reduce(lambda x,y: x+y, [GV.opt['-'+s] and 1 or 0 for s in "crdiI"]):
        pass
    else:
        if not GV.opt['-l']:
            usage('Error: please specify one of -c / -r / -d / -i / -I')
    
    if 1 == len(args):
        GV.snapname = args[0]
        import re
        if not re.match('\A[a-zA-Z0-9][a-zA-Z0-9\-\_\:]*\Z', GV.snapname):
            usage("\n".join(["Error: invalid snapshot name",
                             "Hint: valid name like 'thursday_2009-03_19:00'"]))
    else:
        if not GV.opt['-l']:
            usage('Error: missing snapshot_name')


    
############
def run(cmd):
    vvmsg(cmd)
    p = os.popen(cmd)
    out = p.readlines()
    if GV.opt['-V']:
        for line in out: vvmsg(line[:-1])
    rc = p.close()
    return (rc, out)

def runas(cmd):
    if GV.opt['-u']:
	x = ['su', '-', GV.opt['-u'], '-c', 'bash']
        vvmsg(" ".join(x))
        p = subprocess.Popen(x, 
			     stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE)
	path = os.getenv('PATH')
	ld_library_path = os.getenv('LD_LIBRARY_PATH')
	dyld_library_path = os.getenv('DYLD_LIBRARY_PATH')
	cmd1 = ('export PATH="%s"\n' % path +
		'export LD_LIBRARY_PATH="%s"\n' % ld_library_path +
		'export DYLD_LIBRARY_PATH="%s"\n' % dyld_library_path)
	p.stdin.write(cmd1)
	p.stdin.write(cmd)
	p.stdin.close()
        out = p.stdout.readlines()
        if GV.opt['-V']:
            for line in out: vvmsg(line[:-1])
        return (p.wait(), out)
    else:
        return run(cmd)



def getZfsMountpoint(hostname, datapath): 

    cmd = "echo hostname $(hostname); zfs list -t filesystem " + datapath
    (rc, out) = ssh(hostname, cmd)
    if rc:
        return (None, None)
	
    (hname, mpoint) = (None, None)
    for line in out.split('\n'):
        t = map(lambda x: x.strip(), line.split())
        if not t: continue
        if t[0] == 'hostname': hname = t[1]; continue
        if len(t) != 5: break
        if datapath.find(t[4]) == 0: mpoint = t[0]; continue

    return (hname, mpoint)


class Segment:
    def __init__(self, line):
        row = [x.strip() for x in line.split('|')]
        (self.content, self.definedprimary, self.dbid,
         self.isprimary, self.valid, self.hostname,
         self.port, self.datapath) = row
        (self.toppath, self.dbdir) = os.path.split(strip_trailing_slash(self.datapath))
    def __str__(self):
        return "|".join([self.content, self.definedprimary, self.dbid,
                         self.isprimary, self.valid, self.hostname,
                         self.port, self.datapath])
    

class SnapshotMethod:
    def __init__(self, type, name, seg):
        (self.type, self.name, self.seg) = (type, name, seg)
        self.fullname = ''

    def set_fullname(self,fn): self.fullname = fn
    def createSnapshot(self): return -1
    def deleteSnapshot(self): return -1
    def checkSnapshot(self): return (-1, None)
    def restoreSnapshot(self): return -1

    def type(self): return self.type

class TgzSnapshot(SnapshotMethod):

    def __init__(self, name, seg):
        SnapshotMethod.__init__(self, 'tgz', name, seg)
        self.prefix = '%s@%s' % (seg.dbdir, self.name)
        self.tgzname = self.prefix + '.tgz'
        self.tgzpath = os.path.join(self.seg.toppath, self.tgzname)
        self.set_fullname( (self.seg.hostname, self.tgzpath) )

    def createSnapshot(self):
        msg('Creating snapshot %s:%s' % self.fullname)
        cmd = ('cd %s && ' % self.seg.toppath)
        cmd = cmd + ('rm -f %s && ' % self.tgzname)
        cmd = cmd + ('({ which gtar > /dev/null && TAR=gtar || TAR=tar; } ; $TAR cfz %s %s)' 
                     % (self.tgzname, self.seg.dbdir))
        (rc, out) = ssh(self.seg.hostname, cmd)
        if rc: 
            # some errors occurred ... clean up
            self.deleteSnapshot()
        else:
            msg(' ... created %s:%s' % self.fullname)
        return rc

        
    def deleteSnapshot(self):
        msg('Deleting snapshot %s:%s' % self.fullname)
        cmd = 'rm -f %s' % (self.tgzpath)
        (rc, out) = ssh(self.seg.hostname, cmd)
        return rc


    def checkSnapshot(self):
        msg('Checking snapshot %s:%s' % self.fullname)
        cmd = ('({ which gtar > /dev/null && TAR=gtar || TAR=tar; } ; $TAR tfz %s > /dev/null && ls -l %s) 2>&1' 
			   % (self.tgzpath, self.tgzpath))
        (rc, out) = ssh(self.seg.hostname, cmd)
        lines = out.split('\n')
        return (rc, lines)
        
    def restoreSnapshot(self):
        msg('Restoring snapshot %s:%s' % self.fullname)
        cmd = ('rm -rf %s; cd %s && ({ which gtar > /dev/null && TAR=gtar || TAR=tar; } ; $TAR xfz %s) 2> /dev/null' 
               % (self.seg.datapath, self.seg.toppath, self.tgzname))
        (rc, out) = ssh(self.seg.hostname, cmd)
        return rc
        
    
class ZfsSnapshot(SnapshotMethod):
    '''Redefine the operations on segments using ZFS snapshots.'''

    def __init__(self, name, seg):
        SnapshotMethod.__init__(self, 'zfs', name, seg)
        (hname, mpoint) = getZfsMountpoint(self.seg.hostname, self.seg.datapath)
        if not hname or not mpoint:
            msg('Error: unable to find zfs mountpoint for %s:%s'
                % (self.seg.hostname, self.seg.datapath))
            return None

        # note: hname is slightly different from self.seg.hostname(). 
        # hname is what the shell 'hostname' command returned.
        self.set_fullname( (hname, mpoint + '@' + self.name) )


    def createSnapshot(self):
        (hname, sname) = self.fullname
        cmd = 'zfs snapshot ' + sname
        (rc, out) = ssh(hname, cmd)
        if not rc:
            msg('Created snapshot %s:%s' % self.fullname)
        return rc

    def deleteSnapshot(self):
        (hname, sname) = self.fullname
        msg('Deleting snapshot of %s:%s' % self.fullname)
        cmd = 'zfs destroy %s' % (sname)
        (rc, out) = ssh(hname, cmd)
        return rc
        

    def checkSnapshot(self):
        (hname, sname) = self.fullname
        msg('Checking snapshot of %s:%s' % self.fullname)
        cmd = 'zfs list -t snapshot %s 2>&1' % sname
        (rc, out) = ssh(hname, cmd)
        return (rc, out.split('\n'))
               
    def restoreSnapshot(self):
        (hname, sname) = self.fullname
        msg('Restoring snapshot of %s:%s' % self.fullname)
        cmd = 'zfs rollback ' + sname
        (rc, out) = ssh(hname, cmd)
        return rc
        


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
                raise ex[0], ex[1], ex[2]
            ret.append(result)
    except Queue.Empty: pass
    return ret


def test_pmap():
    import random, time
    def p(x): time.sleep(random.random()); print x; return x
    jlist = [x for x in "abcdefghijklmnopqrstuvwxyz"]
    return pmap(p, jlist)

def ctlpath(dbdir, name):
    home = "~%s" % GV.opt['-u']
    dir = ".gpsnap"
    fname = "%s@%s.sn2" % (dbdir, name)
    return (home, dir, fname)


############
class ControlInfo:
    def __init__(self, name, snapshots, type, etime):
        (self.name, self.snapshots, self.etime) = (name, snapshots, etime)
        self.etime = self.etime.replace(microsecond=0)
        self.type = type;

    def delete(self, master):
        fpath = ctlpath(master.dbdir, self.name)
        fpath = os.path.join(fpath[0], fpath[1], fpath[2])
        cmd = 'rm -f %s' % fpath
        (rc, out) = ssh(master.hostname, cmd)
        return rc

    def write(self, master):
        (home, dir, fname) = ctlpath(master.dbdir, self.name)
        cmd =  ('cd %s && mkdir -p %s && cd %s && cat > %s'
                % (home, dir, dir, fname))
        line = []
        line.append('name: ' + self.name)
        line.append('type: ' + self.type)
        line.append('etime: ' + str(self.etime))
        for i in xrange(len(self.snapshots)):
            line.append('segment' + str(i) + ': ' + str(self.snapshots[i].seg))
        line.append('')

        (rc, out) = ssh(master.hostname, cmd, '\n'.join(line))
        if not rc:
            msg('Control file at %s:%s/%s/%s' % (master.hostname, home, dir, fname))
        return rc

    @staticmethod
    def parse(f):
        dict = {}
        for line in f:
            line = line.strip()
            if len(line) > 0 and line[0] == '#': continue
            line = line.split(':', 1)
            if len(line) != 2: continue
            dict[line[0].strip()] = line[1].strip()
        if not dict.get('name'): return None
        if not dict.get('type'): return None
        if not dict.get('etime'): return None
        if not dict.get('segment0'): return None
        segments = []
        i = 0
        while True:
            n = dict.get('segment' + str(i))
            if not n: break
            segments.append(Segment(n))
            i = i + 1
        
        etime = datetime.datetime.strptime(dict['etime'], '%Y-%m-%d %H:%M:%S')
        snapshots = mkSnapshots(segments, dict['name'], dict['type'])
        return ControlInfo(dict['name'], snapshots, 
                           dict['type'], etime)

    @staticmethod
    def read(name):
        (toppath, dbdir) = os.path.split(MASTER_DATA_DIRECTORY)
        (home, dir, fname) = ctlpath(dbdir, name)
        cmd = ('cd %s && test -d %s && cd %s && test -e %s && cat %s'
               % (home, dir, dir, fname, fname))
        (rc, out) = ssh('localhost', cmd)
        ctl = ControlInfo.parse(out.split('\n'))
        return ctl
        
    
    @staticmethod
    def list():
        (toppath, dbdir) = os.path.split(MASTER_DATA_DIRECTORY)
        fpath = ctlpath(dbdir, '')
        fpath = os.path.join(fpath[0], fpath[1], '*@*.sn2')
        p = os.popen('''bash -c 'ls -1 %s 2> /dev/null' ''' % fpath)
        out = p.readlines()
        p.close()
        
        ret = []
        for fpath in out:
            fpath = fpath.strip()
            f = None
            try: 
                f = open(fpath); 
                x = ControlInfo.parse(f)
                if x: ret.append(x)
            except IOError: pass
            finally: f and f.close()
        
        return ret



def mkSnapshots(segments, name, type):
    segments = segments[:]
    random.shuffle(segments)
    if type == 'zfs':
        snapshots = pmap(lambda s: ZfsSnapshot(name, s), segments)
    else:
        snapshots = map(lambda s: TgzSnapshot(name, s), segments)

    htab = {}
    for (fn, s) in map(lambda s: (s.fullname, s), snapshots):
        htab[fn] = s

    return map(lambda k: htab[k], htab.keys())


############
def pstop(segments):
    def action(hostname):
        cmd = "ps -ef | grep postgres | grep -v grep | awk '{print $2}' | xargs pstop"
        (rc, out) = ssh(hostname, cmd)
        return rc

    masters = filter(lambda s: s.content == '-1', segments)
    segments = filter(lambda s: s.content != '-1', segments)
    vmsg('Suspending Masters')
    pmap(lambda s: action(s.hostname), masters)
    vmsg('Suspending Segments')
    pmap(lambda s: action(s.hostname), segments)
    msg('GPDB suspended.')

############
def prun(segments):
    def action(hostname):
        cmd = "ps -ef | grep postgres | grep -v grep | awk '{print $2}' | xargs prun"
        (rc, out) = ssh(hostname, cmd)
        return rc

    masters = filter(lambda s: s.content == '-1', segments)
    segments = filter(lambda s: s.content != '-1', segments)
    vmsg('Resuming Segments')
    pmap(lambda s: action(s.hostname), segments)
    vmsg('Resuming Masters')
    pmap(lambda s: action(s.hostname), masters)
    msg('GPDB resumed.')
    

############
def createSnapshot(name):
    ctlInfo = ControlInfo.read(name)
    if ctlInfo:
        die("Snapshot '%s' exists." % name)
    
    # need gp_configuration
    print 'Create a %s snapshot.' % (GV.opt['-z'] and 'zfs' or 'tgz')
    print 'Retrieving gp_configuration ... '
    stime = datetime.datetime.now()
    CMD = ("python %s -f -d %s %s"
			% (os.path.join(sys.path[0], 'gpgetconfig.py'), 
			   MASTER_DATA_DIRECTORY, 
			   GV.opt['-u'] and '-u ' + GV.opt['-u'] or ''))
    (rc, out) = runas(CMD)
    if rc: 
	die("Unable to retrieve gp_configuration")

    vvmsg(out)
    out = filter(lambda x: x.find('[gpgetconfig]') == 0, out)
    segments = [Segment(line.split(']', 1)[1]) for line in out]
    
    # shut it down
    suspend = GV.opt['-z']
    if suspend:
        if not confirm('This will suspend the database.'):
            die('Aborted by user.')
        vmsg('Suspending gpdb ...')
        pstop(segments)
    else:
        if not confirm('This will shutdown the database'):
            die('Aborted by user.')
        vmsg("Stopping gpdb ...")
        runas('gpstop -af')

    try:
        # make the snapshots objects
        snapshots = mkSnapshots(segments, name, GV.opt['-z'] and 'zfs' or 'tgz')

        # take the snapshots in parallel
        erows = filter(lambda (s, rc): rc,
                       pmap(lambda s: (s, s.createSnapshot()), snapshots))
        # if error -> rollback
        if erows:
            emsg = (["Create failed:"] +
                    ["  unable to create %s:%s" % s.fullname for (s, rc) in erows])
            # delete the snapshots
            map(lambda s: (s, s.deleteSnapshot()), snapshots)
            #pmap(lambda s: (s, s.deleteSnapshot()), snapshots)
            die("\n".join(emsg))

        # create control file
        etime = datetime.datetime.now()
        ctlInfo = ControlInfo(name, snapshots, GV.opt['-z'] and 'zfs' or 'tgz', etime)

        # write the control file to the masters
        masters = filter(lambda s: s.content == '-1', segments)
        erows = filter(lambda (m, rc): rc,
                       [(m, ctlInfo.write(m)) for m in masters])
        # if error -> rollback
        if erows:
            emsg = (["Create failed:"] +
                    ["  cannot create control file %s:%s"
                     % (m.hostname, "/".join(ctlpath(m.dbdir, name))) for (m, rc) in erows])
            # delete the snapshots
            pmap(lambda s: (s, s.deleteSnapshot()), snapshots)
            pmap(lambda m: (m, ctlInfo.delete(m)), masters)
            die("\n".join(emsg))
        
        msg("Created. Elapsed %s." % humantime(etime - stime))
    finally:
        if suspend:
            prun(segments)

############
def restoreSnapshot(name):
    ctlInfo = ControlInfo.read(name)
    if not ctlInfo:
        die("Unable to find snapshot '%s'" % name)
    
    if not confirm("Restore snapshot '%s'.\nThis will shutdown the database." % name):
        die("Aborted by user.")

    stime = datetime.datetime.now()

    # shut it down
    runas('gpstop -af')

    snapshots = ctlInfo.snapshots[:]
    random.shuffle(snapshots)
    erows = filter(lambda (s,(rc,lines)): rc,
                   pmap(lambda s: (s, s.checkSnapshot()), snapshots))
    
    if erows:
        emsg = (["Snapshot failure"] +
                ["  Bad/missing snapshot of %s:%s" % s.fullname for (s, rc) in erows])
        die("\n".join(emsg))

    if not confirm("\n".join(["\nContinue to *DELETE* database and restore snapshot.",
                              "THIS IS THE POINT OF NO RETURN."])):
        die("Aborted by user.")

    erows = filter(lambda (s,rc): rc, 
                   pmap(lambda s: (s, s.restoreSnapshot()), snapshots))
    if erows:
        emsg = (["Restore failed:"] +
                ["  unable to restore %s:%s" % s.fullname for (s, rc) in erows])
        die("\n".join(emsg))

    etime = datetime.datetime.now()
    msg("Restored. Elapsed %s." % humantime(etime - stime))
    
    
############
def infoSnapshot(name):
    ctlInfo = ControlInfo.read(name)
    if not ctlInfo:
        die("Unable to find snapshot '%s'" % name)
    msg('Snapshot name: ' + ctlInfo.name)
    msg('Created      : ' + str(ctlInfo.etime))
    msg('Atoms        : %s:%s' % ctlInfo.snapshots[0].fullname)
    for s in ctlInfo.snapshots[1:]: msg("               %s:%s" % s.fullname)
    return ctlInfo
    
############
def infoSnapshotDetailed(name):
    ctlInfo = infoSnapshot(name)
    msg('')
    msg('Obtaining for detailed info ...')
    snapshots = ctlInfo.snapshots[:]
    random.shuffle(snapshots)

    erows = []
    for (s, (rc, lines)) in pmap(lambda s: (s, s.checkSnapshot()), snapshots):
	if rc: erows.append( (s, (rc, lines)))
	else: 
	    for x in lines: msg('    [%s:%s info] ' % s.fullname + x.strip())

    for (s, (rc, lines)) in erows:
	    for x in lines: msg('    [%s:%s error] ' % s.fullname + x.strip())

    if erows:
	die("Error(s) detected. Snapshot is not valid.")
	

############
def deleteSnapshot(name):
    ctlInfo = ControlInfo.read(name)
    if not ctlInfo:
        msg("Snapshot '%s' does not exist" % name)
        return                  # assume there isn't any snapshots available

    if not confirm("Delete snapshot '%s'" % name):
        die("Aborted by user.")

    stime = datetime.datetime.now()

    snapshots = ctlInfo.snapshots[:]
    random.shuffle(snapshots)

    erows = filter(lambda (s,rc): rc,
                   pmap(lambda s: (s, s.deleteSnapshot()), snapshots))
    if erows:
        emsg = (["Delete failed:"] +
                ["  unable to delete %s:%s" % s.fullname for (s, rc) in erows])
        die("\n".join(emsg))

    master_snapshots = filter(lambda s: s.seg.content == '-1', snapshots)
    pmap(lambda m: (m.seg, ctlInfo.delete(m.seg)), master_snapshots)
    etime = datetime.datetime.now()
    msg("Deleted. Elapsed %s." % humantime(etime - stime))


############
def listSnapshots(name):
    ctlInfoList = ControlInfo.list()
    if not ctlInfoList:
        msg("No snapshot exists")
        return
    ctlInfoList.sort(lambda x,y: x.etime > y.etime)
    for i in ctlInfoList:
        if not name or name == i.name:
            print '%s %s %s' % (i.type, str(i.etime), i.name)


############
def main():
    global MASTER_DATA_DIRECTORY
    MASTER_DATA_DIRECTORY = strip_trailing_slash(MASTER_DATA_DIRECTORY)
    parseCommandLine()
    if GV.opt['-c']:   createSnapshot(GV.snapname)
    elif GV.opt['-r']: restoreSnapshot(GV.snapname)
    elif GV.opt['-d']: deleteSnapshot(GV.snapname)
    elif GV.opt['-i']: infoSnapshot(GV.snapname)
    elif GV.opt['-I']: infoSnapshotDetailed(GV.snapname)
    elif GV.opt['-l']: listSnapshots(GV.snapname)
    else:
	 usage('Error: invalid flags and/or arguments')

if __name__ == '__main__':
    main()
